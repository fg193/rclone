package receivermaincmd

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"
)

type Fs struct {
	name     string
	root     string
	opts     *Opts
	ci       *fs.ConfigInfo
	features *fs.Features
	rt       *recvTransfer
}

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "rsync",
		Description: "http Connection",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "url",
			Help:     `URL of rsync module, e.g. "rsync://example.com/mod/path"`,
			Required: true,
		}, {
			Name:     "archive",
			Help:     `Archive mode; same as -rlptgoD (no -H)`,
			Advanced: true,
			Default:  true,
		}},
	})
}

// NewFs creates a new Fs object from the name and root. It connects to
// the host specified in the config file.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	f := &Fs{name: name, root: root, opts: new(Opts)}
	err := configstruct.Set(m, f.opts)
	if err != nil {
		return nil, err
	}
	f.opts.Recurse = true
	f.opts.PreserveLinks = true
	f.opts.PreserveTimes = true

	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)

	f.rt, err = f.newConnection()
	return f, err
}

func (f *Fs) newConnection() (rt *recvTransfer, err error) {
	var (
		conn                   io.ReadWriter
		host, path, module     string
		port, daemonConnection int // no daemon
	)
	host, path, port, err = checkForHostspec(f.opts.URL)

	if err != nil {
		// TODO: source is local, check dest arg
		return nil, fmt.Errorf("push not yet implemented")
	} else {
		// source is remote
		if port != 0 {
			if f.opts.ShellCommand != "" {
				daemonConnection = 1 // daemon via remote shell
			} else {
				daemonConnection = -1 // daemon via socket
			}
		}
	}

	negotiate := false
	if daemonConnection < 0 {
		u, err := url.Parse(f.opts.URL)
		if err != nil {
			return nil, err
		}

		host = u.Host
		if _, _, err = net.SplitHostPort(host); err != nil {
			host += ":873" // rsync daemon port
		}

		fs.Debugf(nil, "Opening TCP connection to %s", host)
		if conn, err = net.Dial("tcp", host); err != nil {
			return nil, err
		}

		path = strings.TrimPrefix(u.Path, "/")
		if path == "" {
			return nil, fmt.Errorf("empty remote path")
		}
	} else {
		machine := host
		user := ""
		if idx := strings.IndexByte(machine, '@'); idx > -1 {
			user = machine[:idx]
			machine = machine[idx+1:]
		}
		rc, wc, err := doCmd(f.opts, machine, user, path, daemonConnection)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		defer wc.Close()
		conn = &readWriter{
			Reader: rc,
			Writer: wc,
		}
		negotiate = true
	}

	module = path
	if idx := strings.IndexByte(module, '/'); idx > -1 {
		module = module[:idx]
	}
	fs.Debugf(f, "host=%q, module=%q, path=%q, port=%d, daemon=%d",
		host, module, path, port, daemonConnection)
	if daemonConnection != 0 {
		if err := startInbandExchange(f.opts, conn, module, path); err != nil {
			return nil, err
		}
		negotiate = false // already done
	}

	if rt, err = newRecvTransfer(osenv{}, f.opts, conn, "", negotiate); err != nil {
		return nil, err
	}
	return rt, nil
}

// Name returns the configured name of the file system
func (f *Fs) Name() string {
	return f.name
}

// Root returns the root for the filesystem
func (f *Fs) Root() string {
	return f.root
}

// String returns the URL for the filesystem
func (f *Fs) String() string {
	return f.name
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision is the remote http file system's modtime precision, which we have no way of knowing. We estimate at 1s
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Hashes returns hash.HashNone to indicate remote hashing is unavailable
// TODO: return hash.MD4
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// Mkdir makes the root directory of the Fs object
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return errorReadOnly
}

// Rmdir removes the root directory of the Fs object
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return errorReadOnly
}

// NewObject fetches a remote file object
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fs.Debugf(f, "newObj %s", remote)
	o := object{&file{
		fs:   f,
		Name: remote,
	}}
	return o, nil
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	fileList, err := f.rt.receiveFileList()
	if err != nil {
		return
	}

	entries = make(fs.DirEntries, 0, len(fileList))
	for _, file := range fileList {
		file.fs = f
		if file.FileMode().IsDir() {
			entries = append(entries, directory{file})
		} else {
			entries = append(entries, object{file})
		}
	}
	return
}

// ListR lists the objects and directories of the Fs starting
// from dir recursively into out.
//
// dir should be "" to start from the root, and should not
// have trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
//
// It should call callback for each tranche of entries read.
// These need not be returned in any particular order.  If
// callback returns an error then the listing will stop
// immediately.
//
// Don't implement this unless you have a more efficient way
// of listing recursively than doing a directory traversal.
func (f *Fs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) (err error) {
	fileList, err := f.rt.receiveFileList()
	if err != nil {
		return
	}

	entries := make(fs.DirEntries, 0, len(fileList))
	for _, file := range fileList {
		file.fs = f
		if file.FileMode().IsDir() {
			entries = append(entries, directory{file})
		} else {
			entries = append(entries, object{file})
		}
	}
	return callback(entries)
}

// Put in to the remote path with the modTime given of the given size
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, errorReadOnly
}
