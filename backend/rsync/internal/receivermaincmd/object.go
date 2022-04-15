package receivermaincmd

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/mmcloughlin/md4"
	"github.com/rclone/rclone/backend/rsync"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"golang.org/x/sync/errgroup"
)

type object struct {
	*file
}

type directory struct {
	*file
}

var (
	errorReadOnly = fmt.Errorf("rsync remote is read only")

	_ fs.DirEntry  = new(file)
	_ fs.Object    = new(object)
	_ fs.Directory = new(directory)
)

// Fs is the filesystem this remote http file object is located within
func (o *file) Fs() fs.Info {
	return o.fs
}

// String returns the URL to the remote HTTP file
func (o *file) String() string {
	return o.Name
}

// Remote the name of the remote HTTP file, relative to the fs root
func (o *file) Remote() string {
	return o.Name
}

// Hash TODO: return hash.MD4
func (o *file) Hash(ctx context.Context, r hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Size returns the size in bytes of the remote http file
func (o *file) Size() int64 {
	return o.Length
}

// ModTime returns the modification time of the remote http file
func (o *file) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// SetModTime sets the modification and access time to the specified time
func (o object) SetModTime(ctx context.Context, modTime time.Time) error {
	return errorReadOnly
}

// Storable returns whether the remote rsync file is a regular file (not a directory, symbolic link, block device, character device, named pipe, etc.)
func (o object) Storable() bool {
	return o.FileMode().IsRegular()
}

// Open a remote rsync file for reading
func (o object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	eg, ctx := errgroup.WithContext(ctx)

	// generator
	eg.Go(func() error {
		fs.Debugf(o, "requesting file idx=%d", o.index)

		if err := o.fs.rt.conn.WriteInt32(int32(o.index)); err != nil {
			return err
		}

		// write empty checksum to receive the whole file
		var sh rsync.SumHead
		return sh.WriteTo(o.fs.rt.conn)
	})

	if err = eg.Wait(); err != nil {
		return
	}
	pipeReader, pipeWriter := io.Pipe()
	idx, err := o.fs.rt.conn.ReadInt32()
	if err != nil {
		return nil, err
	}
	fs.Debugf(o, "receiving file idx=%d", idx)

	eg.Go(func() error {
		return pipeWriter.CloseWithError(o.recvFile(pipeWriter))
	})
	return pipeReader, nil
}

func (o *object) recvFile(pipeWriter io.WriteCloser) error {
	var sh rsync.SumHead
	if err := sh.ReadFrom(o.fs.rt.conn); err != nil {
		return err
	}

	h := md4.New()
	binary.Write(h, binary.LittleEndian, o.fs.rt.seed)

	multiWriter := io.MultiWriter(pipeWriter, h)

	for {
		token, data, err := o.fs.rt.recvToken()
		if err != nil {
			return err
		}
		if token == 0 {
			break
		}
		if token > 0 {
			if _, err := multiWriter.Write(data); err != nil {
				return err
			}
			continue
		}

		token = -(token + 1)
		dataLen := sh.BlockLength
		if token == sh.ChecksumCount-1 && sh.RemainderLength != 0 {
			dataLen = sh.RemainderLength
		}
		data = make([]byte, dataLen)

		if _, err := multiWriter.Write(data); err != nil {
			return err
		}
	}

	localSum := h.Sum(nil)
	remoteSum := make([]byte, len(localSum))
	if _, err := io.ReadFull(o.fs.rt.conn.Reader, remoteSum); err != nil {
		return err
	}
	if bytes.Equal(localSum, remoteSum) {
		fs.Debugf(o, "checksum %s matches", hex.EncodeToString(localSum))
		return nil
	}
	return fmt.Errorf("checksum mismatch: idx=%d name=%q local=%s remote=%s",
		o.index, o.Name, hex.EncodeToString(localSum), hex.EncodeToString(remoteSum))
}

// Remove a remote rsync file
func (o object) Remove(ctx context.Context) error {
	return errorReadOnly
}

// Update in to the object with the modTime given of the given size
func (o object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return errorReadOnly
}

// Items returns the count of items in this directory or this
// directory and subdirectories if known, -1 for unknown
func (d directory) Items() int64 {
	return -1
}

// ID returns the internal ID of this directory if known, or
// "" otherwise
func (d directory) ID() string {
	return ""
}
