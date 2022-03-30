// Package codeup implements a local-database-based file metadata overlay backend
package codeup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/rclone/rclone/cmd/serve/http/data"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/rest"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "codeup",
		Description: "Alicloud CodeUp Package Registry, with metadata in local SQL database",
		NewFs:       NewFs,
		CommandHelp: []fs.CommandHelp{{
			Name:  "serve",
			Short: "Serve the remote over HTTP",
			Long: `Usage Examples:

    rclone backend serve remote: -o ListenAddr=0.0.0.0:8080 -o Template=_site/fancy-index/index.html
			` + data.Help,
			Opts: map[string]string{
				"Template":           "Path to the HTML template for directory indexing",
				"ListenAddr":         "Port to listen on",
				"BaseURL":            "Prefix to strip from URLs",
				"ServerReadTimeout":  "Timeout for server reading data",
				"ServerWriteTimeout": "Timeout for server writing data",
				"SslCert":            "Path to SSL PEM key (concatenation of server cert and CA cert)",
				"SslKey":             "Path to SSL PEM Private key",
				"ClientCA":           "Client certificate authority to verify clients with",
			},
		}},
		Options: []fs.Option{{
			Name: "org",
			Help: `The organization ID.

Organization ID should be 24 hexadecimal digits.

It presents in the URL path of the repository, which is shown on the
page "https://packages.aliyun.com/generic/flow_generic_repo/setting",
like "https://package.aliyun.com/{organizationID}/npm/npm-registry".`,
		}, {
			Name: "user",
			Help: `The user ID for Alicloud Package Registry.

User ID should be 24 hexadecimal digits.

You may get it from "https://package.aliyun.com/npm/npm-registry/guide" or
"https://packages.aliyun.com/system-settings".
Do not confuse it with the organization ID, which is in the repository URL.`,
		}, {
			Name: "password",
			Help: `The password for Alicloud Package Registry.

You may get it from "https://package.aliyun.com/npm/npm-registry/guide" or
reset it at https://packages.aliyun.com/system-settings`,
		}, {
			Name: "database",
			Help: `The DSN of the PostgreSQL metadata database.`,
		}, {
			Name: "weak_link",
			Help: `Enable the weak link directory property.

When a nonexistent file path is visited in a directory with
the weak link property set, instead of ENOENT, a default symbolic link
will be generated and returned based on the weak link.  `,
			Default:  true,
			Advanced: true,
		}, {
			Name:     "max_inline_size",
			Help:     `Files smaller than this threshold will be saved in the local database.`,
			Default:  1 * fs.Kibi,
			Advanced: true,
		}, {
			Name:     "hashes",
			Help:     `List of supported hash types, splitted by commas.`,
			Default:  "md5,sha1,sha256,crc64ecma",
			Advanced: true,
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	Org      string `config:"org"`
	User     string `config:"user"`
	Password string `config:"password"`
	Database string `config:"database"`
	WeakLink bool   `config:"weak_link"`

	MaxInlineSize fs.SizeSuffix   `config:"max_inline_size"`
	Hashes        fs.CommaSepList `config:"hashes"`
}

// Fs represents the SQL-based metadata storage
type Fs struct {
	// name of this remote
	Remote   string       `gorm:"notNull;uniqueIndex:idx_parent_name"`
	root     string       // the path we are working on if any
	opts     Options      // parsed config options
	features *fs.Features // optional features
	hashSet  hash.Set     // supported hash types
	httpCli  *rest.Client // the rest connection to the server
	db       *gorm.DB     // metadata database
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.Remote
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("CodeUp Package Registry %q", f.Remote)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	root = strings.Trim(root, "/")
	if len(root) > 0 {
		root += "/"
	}

	f := &Fs{
		Remote: name,
		root:   root,
	}
	err := configstruct.Set(m, &f.opts)
	if err != nil {
		return nil, err
	}

	for _, hashName := range f.opts.Hashes {
		var ht hash.Type
		if err := ht.Set(hashName); err != nil {
			return nil, fmt.Errorf("invalid hash type %q", hashName)
		}
		f.hashSet.Add(ht)
	}

	f.httpCli = rest.NewClient(http.DefaultClient)
	f.httpCli.SetRoot(fmt.Sprintf("https://packages.aliyun.com/api/protocol/%s/GENERIC/flow_generic_repo/files/", f.opts.Org))
	f.httpCli.SetUserPass(f.opts.User, f.opts.Password)

	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		DuplicateFiles:          true,
		ReadMimeType:            true,
		WriteMimeType:           true,
	}).Fill(ctx, f)

	if f.db, err = gorm.Open(postgres.Open(f.opts.Database), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger: logger.Default.LogMode(
			f.dbLogLevel(ctx)),
		PrepareStmt: true,
	}); err != nil {
		return nil, err
	}
	if err = f.db.AutoMigrate(new(Object)); err != nil {
		return nil, err
	}
	return f, nil
}

// dbLogLevel maps rclone global log levels to database log levels
func (f *Fs) dbLogLevel(ctx context.Context) logger.LogLevel {
	ci := fs.GetConfig(ctx)
	switch {
	case ci.LogLevel <= fs.LogLevelCritical:
		return logger.Silent
	case ci.LogLevel <= fs.LogLevelError:
		return logger.Error
	case ci.LogLevel <= fs.LogLevelNotice:
		return logger.Warn
	default:
		return logger.Info
	}
}

// getObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) getObject(ctx context.Context, remote string, tryWeakLink bool) (*Object, error) {
	parent, name := path.Split(path.Join(f.root, remote))
	name = strings.TrimSuffix(name, fs.LinkSuffix)
	if len(name) <= 0 {
		// root directory special case
		if len(parent) <= 0 {
			return &Object{FS: f, Parent: &parent, Mode: os.ModeDir}, nil
		}
		return nil, fs.ErrorNotAFile
	}

	var o Object
	ret := f.db.
		WithContext(ctx).
		Find(&o, &Object{FS: f, Parent: &parent, FileName: name})
	if ret.Error != nil {
		return nil, ret.Error
	}
	if ret.RowsAffected <= 0 {
		if tryWeakLink {
			return f.getWeakLink(ctx, parent, name)
		}
		return nil, fs.ErrorObjectNotFound
	}

	o.FS = f
	return &o, nil
}

// getWeakLink generates a symbolic link based on the weak link rules
func (f *Fs) getWeakLink(ctx context.Context, parent, name string) (*Object, error) {
	if !f.opts.WeakLink || len(parent) == 0 {
		return nil, fs.ErrorObjectNotFound
	}

	o := Object{
		FS:       f,
		Parent:   new(string),
		FileName: name,
		Mode:     os.ModeSymlink,
	}
	*o.Parent = parent

	// search for the nearest weak link in the ancestor hierarchy
	cond := f.db
	for len(parent) > 0 {
		parent, name = path.Split(path.Clean(parent))
		parent := parent
		cond = cond.Or(&Object{Parent: &parent, FileName: name})
	}

	var dir Object
	ret := f.db.
		WithContext(ctx).
		Where(&Object{FS: f, Mode: os.ModeDir}).
		Where(cond).
		Where("contents is not null").
		Order("octet_length(parent) desc").
		Limit(1).
		Find(&dir)
	if ret.RowsAffected <= 0 {
		return nil, fs.ErrorObjectNotFound
	}
	if ret.Error != nil {
		return nil, ret.Error
	}

	relativeRoot := path.Join(*dir.Parent, dir.FileName)
	if len(relativeRoot) > 0 {
		relativeRoot += "/"
	}
	relativePath := path.Join(*o.Parent, o.FileName)[len(relativeRoot):]

	rules := string(dir.Contents)
	targetPath := relativePath
	if err := f.readWeakLink(rules[:1], rules[1:], &targetPath); err != nil {
		return nil, err
	}

	// prevent redirect loop
	if targetPath == relativePath {
		return nil, fs.ErrorObjectNotFound
	}
	fs.Infof(f, "weak link %s{%s -> %s}", relativeRoot, relativePath, targetPath)
	o.Contents = []byte(targetPath)
	return &o, nil
}

// readWeakLink execute the substitution rules in the weak link property
func (f *Fs) readWeakLink(delimiter, rules string, targetPath *string) error {
	for len(rules) > 0 {
		components := strings.SplitN(rules, delimiter, 3)
		switch len(components) {
		case 3:
			rules = components[2]
		case 2:
			rules = ""
		default:
			return nil
		}

		pattern, err := regexp.Compile(components[0])
		if err != nil {
			return err
		}
		*targetPath = pattern.ReplaceAllString(*targetPath, components[1])
	}
	return nil
}

// NewObject finds the regular file of symbolic link at remote.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	o, err := f.getObject(ctx, remote, false)
	if err != nil {
		return nil, err
	}

	if o.Mode.IsDir() {
		return nil, fs.ErrorIsDir
	}

	return &RegularFile{*o}, nil
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
	objects, err := f.list(ctx, dir)
	if err != nil {
		return
	}
	return f.toDirEntries(objects), nil
}

func (f *Fs) list(ctx context.Context, dir string) (objects []*Object, err error) {
	dir = path.Join(f.root, dir)
	if len(dir) > 0 {
		dir += "/"
	}
	err = f.db.
		WithContext(ctx).
		Select(objectListColumns).
		Find(&objects, &Object{FS: f, Parent: &dir}).
		Error

	for _, o := range objects {
		o.FS = f
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
// of listing recursively that doing a directory traversal.
func (f *Fs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) (err error) {
	var objects []*Object
	dir = path.Join(f.root, dir)
	if len(dir) > 0 {
		dir += "/"
	}
	err = f.db.
		WithContext(ctx).
		Select(objectListColumns).
		Where(&Object{FS: f}).
		Find(&objects, "starts_with(parent, ?)", dir).
		Error
	if err != nil {
		return err
	}

	for _, o := range objects {
		o.FS = f
	}
	return callback(f.toDirEntries(objects))
}

func (f *Fs) toDirEntries(objects []*Object) (entries fs.DirEntries) {
	entries = make(fs.DirEntries, 0, len(objects))
	for _, o := range objects {
		if o.Mode.IsDir() {
			entries = append(entries, &Directory{*o})
		} else {
			entries = append(entries, &RegularFile{*o})
		}
	}
	return
}

// Put the object into the bucket
//
// Copy the reader in to the new object which is returned
//
// The new object may have been created if an error is returned
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.PutStream(ctx, in, src, options...)
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (_ fs.Object, err error) {
	// Temporary Object under construction
	fullPath := path.Join(f.root, src.Remote())
	parent, name := path.Split(fullPath)
	o := Object{
		FS:       f,
		Parent:   &parent,
		FileName: name,
		FileSize: src.Size(),
		MTime:    src.ModTime(ctx).UnixNano(),
		Contents: escape(fullPath),
		ID:       time.Now().UnixNano(),
	}

	if strings.HasSuffix(name, fs.LinkSuffix) {
		o.Mode = os.ModeSymlink
		o.FileName = o.FileName[:len(o.FileName)-len(fs.LinkSuffix)]
		return f.putInline(ctx, &o, in)
	}
	if 0 <= src.Size() && src.Size() <= int64(f.opts.MaxInlineSize) {
		o.Mode = os.ModeCharDevice
		return f.putInline(ctx, &o, in)
	}

	for _, ht := range f.hashSet.Array() {
		var digest string
		digest, err = src.Hash(ctx, ht)
		o.Hashes.Set(ht, digest)
	}

	o.Version = time.Now().UnixNano()
	params := NewVersionParams(o.Version)
	params.Set("fileName", o.FileName)
	params.Set("downloadFileName", o.FileName)

	pipeReader, pipeWriter := io.Pipe()
	multiPartWriter := multipart.NewWriter(pipeWriter)
	var (
		resp CreateVersionResponse
		opts = rest.Opts{
			Method:      http.MethodPost,
			Path:        parent,
			Parameters:  params,
			ContentType: multiPartWriter.FormDataContentType(),
			Body:        pipeReader,
		}
	)

	// in -> fileWriter ->  multiPartWriter -> pipeWriter -> pipeReader
	go func() {
		fileWriter, err := multiPartWriter.CreateFormFile("file", o.FileName)
		if err != nil {
			return
		}
		o.FileSize, err = io.Copy(fileWriter, in)
		multiPartWriter.Close()
		pipeWriter.Close()
	}()

	_, err = f.httpCli.CallJSON(ctx, &opts, nil, &resp)
	if err != nil {
		if o.FileSize > 0 {
			return nil, err
		}

		// PutStream an empty file
		// codeUp always returns 400 if file is empty
		// TODO: skip this useless request
		o.Mode = os.ModeCharDevice
		o.Contents = nil
		o.Version = 0
	}

	o.Hashes = resp.Object.Hashes
	if src.Size() != resp.Object.FileSize {
		fs.Logf(&o, "local size %d != received size %d",
			src.Size(), resp.Object.FileSize)
	}
	if o.FileSize != resp.Object.FileSize {
		fs.Logf(&o, "sent size %d != received size %d",
			o.FileSize, resp.Object.FileSize)
		o.FileSize = resp.Object.FileSize
	}

	r := &RegularFile{o}
	prevObj, err := f.NewObject(ctx, o.Remote())
	if errors.Is(err, fs.ErrorObjectNotFound) {
		// create new file
	} else if err != nil {
		// should not get here
		r.unlink(ctx)
		return r, err
	} else if err = prevObj.Remove(ctx); err != nil {
		// overwrite existing file
		r.unlink(ctx)
		return r, err
	}
	err = f.db.WithContext(ctx).Create(&o).Error
	return r, err
}

func (f *Fs) putInline(ctx context.Context, o *Object, in io.Reader) (_ fs.Object, err error) {
	contents, err := ioutil.ReadAll(in)
	if err != nil {
		return
	}
	o.Contents = contents

	r := &RegularFile{*o}
	prevObj, err := f.NewObject(ctx, o.Remote())
	if errors.Is(err, fs.ErrorObjectNotFound) {
		// create new link
	} else if err != nil {
		// should not get here
		return r, err
	} else if err = prevObj.Remove(ctx); err != nil {
		// overwrite existing link
		return r, err
	}
	err = f.db.WithContext(ctx).Create(&o).Error
	return r, err
}

// Mkdir creates the directory if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	parent, name := path.Split(path.Join(f.root, dir))
	o := Object{
		FS:       f,
		Parent:   &parent,
		FileName: name,
		Mode:     os.ModeDir,
		MTime:    time.Now().UnixNano(),
		ID:       time.Now().UnixNano(),
	}
	return f.db.WithContext(ctx).Create(&o).Error
}

// Rmdir deletes the bucket if the fs is at the root
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) (err error) {
	var children int64
	dir = path.Join(f.root, dir)
	parent, name := path.Split(dir)
	if len(dir) > 0 {
		dir += "/"
	}
	err = f.db.
		WithContext(ctx).
		Model(new(Object)).
		Where(&Object{FS: f, Parent: &dir}).
		Limit(1).
		Count(&children).
		Error
	if err != nil {
		return err
	}
	if children > 0 {
		return fs.ErrorDirectoryNotEmpty
	}

	ret := f.db.
		WithContext(ctx).
		Delete(new(Object), &Object{FS: f, Parent: &parent, FileName: name, Mode: os.ModeDir})
	if ret.Error != nil {
		return ret.Error
	}
	if ret.RowsAffected == 0 {
		return fs.ErrorDirNotFound
	}
	return nil
}

// Precision of the remote
func (f *Fs) Precision() time.Duration {
	return time.Nanosecond
}

// Copy src to this remote using server-side copy operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcFile, ok := src.(*RegularFile)
	if !ok {
		return nil, fs.ErrorCantCopy
	}

	parent, name := path.Split(path.Join(f.root, remote))
	name = strings.TrimSuffix(name, fs.LinkSuffix)

	var destObj Object
	destObj = *&srcFile.Object
	destObj.ID = time.Now().UnixNano()
	destObj.Parent = &parent
	destObj.FileName = name
	err := f.db.WithContext(ctx).Create(&destObj).Error
	return &RegularFile{destObj}, err
}

// Move src to this remote using server-side move operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	file, ok := src.(*RegularFile)
	if !ok {
		return nil, fs.ErrorCantMove
	}

	parent, name := path.Split(path.Join(f.root, remote))
	file.Parent = &parent
	file.FileName = strings.TrimSuffix(name, fs.LinkSuffix)
	err := f.db.Updates(&file.Object).Error
	return file, err
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return f.hashSet
}

// PublicLink generates a public link to the remote path (usually readable by anyone)
func (f *Fs) PublicLink(ctx context.Context, remote string, expire fs.Duration, unlink bool) (link string, err error) {
	if unlink {
		return link, fs.ErrorNotImplemented
	}

	o, err := f.NewObject(ctx, remote)
	if err != nil {
		return
	}

	if o.Size() <= 0 {
		return link, fs.ErrorCantUploadEmptyFiles
	}

	return o.(*RegularFile).getLink(ctx)
}

// Command the backend to run a named command
//
// The command run is name
// args may be used to read arguments from
// opts may be used to read optional arguments from
//
// The result should be capable of being JSON encoded
// If it is a string or a []string it will be shown to the user
// otherwise it will be JSON encoded and shown to the user like that
func (f *Fs) Command(ctx context.Context, name string, args []string, opts map[string]string) (out interface{}, err error) {
	switch name {
	case "serve":
		return nil, f.serve(ctx, opts)
	default:
		return nil, fs.ErrorCommandNotFound
	}
}

// ------------------------------------------------------------

// Object describes a file, either a regular file or a directory
type Object struct {
	ID       int64       `gorm:"notNull;primaryKey;autoIncrement"`
	FS       *Fs         `gorm:"embedded"`
	Parent   *string     `gorm:"notNull;uniqueIndex:idx_parent_name"`
	FileName string      `gorm:"notNull;uniqueIndex:idx_parent_name;column:name"`
	FileSize int64       `gorm:"notNull;column:size"`
	Mode     os.FileMode `gorm:"notNull"`
	MTime    int64       `gorm:"notNull"`
	MIMEType string      `gorm:"default:null"`
	Hashes   Hashes      `gorm:"embedded;embeddedPrefix:hash_"`
	Contents []byte      `gorm:"default:null"`
	Version  int64       `gorm:"default:null"`
}

var objectListColumns = []string{
	"id",
	"parent",
	"name",
	"size",
	"mode",
	"m_time",
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.FS
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.Remote()
}

// Remote returns the remote path
func (o *Object) Remote() string {
	fileName := o.FileName
	if o.Mode&os.ModeSymlink > 0 {
		fileName += fs.LinkSuffix
	}
	return o.remote(fileName)
}

func (o *Object) remote(fileName string) string {
	return strings.TrimPrefix(path.Join(*o.Parent, fileName), o.FS.root)
}

// Hash returns the hash of an object returning a lowercase hex string
func (o *Object) Hash(ctx context.Context, ht hash.Type) (digest string, err error) {
	if !o.FS.hashSet.Contains(ht) {
		return digest, hash.ErrUnsupported
	}
	digest = o.Hashes.Get(ht)
	return
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.FileSize
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
//
// SHA-1 will also be updated once the request has completed.
func (o *Object) ModTime(ctx context.Context) (result time.Time) {
	return time.Unix(0, o.MTime)
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	o.MTime = modTime.UnixNano()
	return o.FS.db.WithContext(ctx).Updates(o).Error
}

func (o *Object) lazyLoad(ctx context.Context) error {
	if o.FileSize == 0 || len(o.Contents) > 0 {
		return nil
	}
	return o.FS.db.WithContext(ctx).First(o, o.ID).Error
}

// ------------------------------------------------------------

// RegularFile describes a regular file
type RegularFile struct {
	Object
}

// Storable returns if this file is storable
func (RegularFile) Storable() bool {
	return true
}

func (r *RegularFile) getLink(ctx context.Context) (link string, err error) {
	if err = r.lazyLoad(ctx); err != nil {
		return
	}
	opts := rest.Opts{
		Method:       http.MethodGet,
		Path:         string(r.Contents),
		Parameters:   NewVersionParams(r.Object.Version),
		NoRedirect:   true,
		IgnoreStatus: true,
	}
	resp, err := r.FS.httpCli.Call(ctx, &opts)
	if err != nil {
		return
	}

	resp.Body.Close()
	if resp.StatusCode < http.StatusMovedPermanently ||
		http.StatusPermanentRedirect < resp.StatusCode {
		return link, fmt.Errorf("expected redirect, got %d %s", resp.StatusCode, resp.Status)
	}

	url, err := resp.Location()
	if err != nil {
		return
	}
	link = url.String()
	return
}

// Open an object for read
func (r *RegularFile) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	if !r.Mode.IsRegular() {
		if err = r.lazyLoad(ctx); err != nil {
			return
		}
		return ioutil.NopCloser(bytes.NewReader(r.Contents)), nil
	}

	if r.FileSize <= 0 {
		return nil, nil
	}

	opts := rest.Opts{
		Method:  http.MethodGet,
		Options: options,
	}
	if opts.RootURL, err = r.getLink(ctx); err != nil {
		return
	}
	resp, err := r.FS.httpCli.Call(ctx, &opts)
	return resp.Body, err
}

// Update the object with the contents of the io.Reader, modTime and size
//
// The new object may have been created if an error is returned
func (r *RegularFile) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	updated, err := r.FS.Put(ctx, in, src, options...)
	if err != nil {
		return
	}

	r = updated.(*RegularFile)
	r.FileSize = src.Size()
	r.SetModTime(ctx, src.ModTime(ctx))
	r.MIMEType = fs.MimeType(ctx, src)
	return r.FS.db.WithContext(ctx).Updates(&r.Object).Error
}

// Remove decreases the version reference count by one,
// which is the opposite action of Copy.
func (r *RegularFile) Remove(ctx context.Context) (err error) {
	// check object exists
	var o fs.Object
	if o, err = r.FS.NewObject(ctx, r.Remote()); err != nil {
		return err
	}
	r = o.(*RegularFile)

	if err = r.unlink(ctx); err != nil {
		return err
	}
	return r.FS.db.WithContext(ctx).Delete(&r.Object, &r).Error
}

// unlink checks whether the version reference count is no more than one.
// If so, remove this version from the object storage; otherwise do nothing.
func (r *RegularFile) unlink(ctx context.Context) (err error) {
	if r.FileSize <= 0 || !r.Mode.IsRegular() {
		return
	}

	count, err := r.refCount(ctx)
	if err != nil || count > 1 {
		return
	}

	var (
		resp DeleteVersionResponse
		opts = rest.Opts{
			Method:     http.MethodDelete,
			Path:       string(r.Contents),
			Parameters: NewVersionParams(r.Version),
		}
	)
	_, err = r.FS.httpCli.CallJSON(ctx, &opts, nil, &resp)
	return
}

func (o *Object) refCount(ctx context.Context) (count int64, err error) {
	err = o.FS.db.
		WithContext(ctx).
		Model(o).
		Where(&Object{FS: o.FS, Hashes: o.Hashes}).
		Count(&count).
		Error
	return
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType(ctx context.Context) string {
	return o.MIMEType
}

// ------------------------------------------------------------

// Directory describes a directory
type Directory struct {
	Object
}

func (d *Directory) Items() (count int64) {
	dir := path.Join(*d.Object.Parent, d.Object.FileName)
	d.Object.FS.db.
		Where(&Object{FS: d.Object.FS, Parent: &dir}).
		Count(&count)
	return
}

func (d *Directory) ID() (id string) {
	return
}

// ------------------------------------------------------------

type Hashes struct {
	MD5       string `hash:"md5" json:"fileMd5" header:"ETag" gorm:"default:null;index:idx_hash_md5"`
	SHA1      string `hash:"sha1" json:"fileSha1" gorm:"default:null"`
	SHA256    string `hash:"sha256" json:"fileSha256" gorm:"default:null"`
	CRC64ECMA string `hash:"crc64ecma" header:"x-oss-hash-crc64ecma" gorm:"default:null"`
}

func (h *Hashes) Get(ht hash.Type) string {
	if h == nil {
		return ""
	}
	hashName := ht.String()
	st := reflect.TypeOf(*h)
	for i := 0; i < st.NumField(); i++ {
		field := st.Field(i)
		if tag, ok := field.Tag.Lookup("hash"); ok && tag == hashName {
			return reflect.ValueOf(*h).Field(i).Interface().(string)
		}
	}
	return ""
}

func (h *Hashes) Set(ht hash.Type, digest string) {
	if h == nil {
		return
	}
	hashName := ht.String()
	st := reflect.TypeOf(*h)
	for i := 0; i < st.NumField(); i++ {
		field := st.Field(i)
		if tag, ok := field.Tag.Lookup("hash"); ok && tag == hashName {
			reflect.ValueOf(h).Elem().Field(i).SetString(digest)
			return
		}
	}
}

// Check the interfaces are satisfied
var (
	_ fs.Fs           = &Fs{}
	_ fs.Copier       = &Fs{}
	_ fs.Mover        = &Fs{}
	_ fs.ListRer      = &Fs{}
	_ fs.PutStreamer  = &Fs{}
	_ fs.PublicLinker = &Fs{}
	_ fs.Commander    = &Fs{}
	_ fs.Object       = &RegularFile{}
	_ fs.MimeTyper    = &Object{}
)
