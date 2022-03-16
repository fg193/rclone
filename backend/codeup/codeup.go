// Package codeup implements a local-database-based file metadata overlay backend
package codeup

import (
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
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/rest"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const linkSuffix = ".rclonelink"

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "codeup",
		Description: "Alicloud CodeUp Package Registry, with metadata in local SQL database",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name: "org",
			Help: `The organization ID.

Organization ID should be 24 hexadecimal digits.

It presents in the URL path of the NPM registry, which is shown on the
NPM registry guide page "https://package.aliyun.com/npm/npm-registry/guide",
like "https://package.aliyun.com/{organizationID}/npm/npm-registry".`,
		}, {
			Name: "user",
			Help: `The user ID for Alicloud Package Registry.

User ID should be 24 hexadecimal digits.

You may get it from "https://package.aliyun.com/npm/npm-registry/guide" or
"https://packages.aliyun.com/system-settings".`,
		}, {
			Name: "password",
			Help: `The password for Alicloud Package Registry.

You may get it from "https://package.aliyun.com/npm/npm-registry/guide" or
reset it at https://packages.aliyun.com/system-settings`,
		}, {
			Name: "database",
			Help: `The path of the local metadata database.`,
		}, {
			Name: "hashes",
			Help: `List of supported hash types, splitted by comma.`,

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

	Hashes fs.CommaSepList `config:"hashes"`
}

// Fs represents the SQL-based metadata storage
type Fs struct {
	Remote   string       `gorm:"notNull;uniqueIndex:idx_parent_name;uniqueIndex:idx_real_path_version"` // name of this remote
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

	if f.db, err = gorm.Open(sqlite.Open(f.opts.Database), &gorm.Config{
		SkipDefaultTransaction: true,
	}); err != nil {
		return nil, err
	}
	if err = f.db.AutoMigrate(new(Object)); err != nil {
		return nil, err
	}
	return f, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	parent, name := path.Split(path.Join(f.root, remote))
	name = strings.TrimSuffix(name, linkSuffix)

	var o Object
	ret := f.db.
		WithContext(ctx).
		Find(&o, &Object{FS: f, Parent: &parent, FileName: name})
	if ret.RowsAffected <= 0 {
		return nil, fs.ErrorObjectNotFound
	}
	if o.Mode.IsDir() {
		return nil, fs.ErrorIsDir
	}

	o.FS = f
	return &RegularFile{o}, ret.Error
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
	var objects []Object
	dir = path.Join(f.root, dir)
	if len(dir) > 0 {
		dir += "/"
	}
	err = f.db.
		WithContext(ctx).
		Find(&objects, &Object{FS: f, Parent: &dir}).
		Error
	if err != nil {
		return
	}

	return f.toDirEntries(objects), nil
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
	var objects []Object
	dir = path.Join(f.root, dir)
	if len(dir) > 0 {
		dir += "/"
	}
	err = f.db.
		WithContext(ctx).
		Where(&Object{FS: f}).
		Find(&objects, "parent GLOB ?", dir+"*").
		Error
	if err != nil {
		return err
	}

	return callback(f.toDirEntries(objects))
}

func (f *Fs) toDirEntries(objects []Object) (entries fs.DirEntries) {
	entries = make(fs.DirEntries, 0, len(objects))
	for _, o := range objects {
		o.FS = f
		if o.Mode.IsDir() {
			entries = append(entries, &Directory{o})
		} else {
			entries = append(entries, &RegularFile{o})
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
		RealPath: fullPath,
		Version:  time.Now().UnixNano(),
	}

	if strings.HasSuffix(name, linkSuffix) {
		return f.putSymLink(ctx, &o, in)
	}

	for _, ht := range f.hashSet.Array() {
		var digest string
		digest, err = src.Hash(ctx, ht)
		o.Hashes.Set(ht, digest)
	}

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

		// codeUp always returns 400 if file is empty
		// TODO: skip this useless request
		o.RealPath = ""
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
	err = f.db.Create(&o).Error
	return r, err
}

func (f *Fs) putSymLink(ctx context.Context, o *Object, in io.Reader) (_ fs.Object, err error) {
	o.Mode = os.ModeSymlink
	o.FileName = o.FileName[:len(o.FileName)-len(linkSuffix)]
	readLink, err := ioutil.ReadAll(in)
	if err != nil {
		return
	}
	o.RealPath = string(readLink)

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
	err = f.db.Create(&o).Error
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
	}
	return f.db.Create(&o).Error
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

	ret := f.db.Delete(new(Object), &Object{FS: f, Parent: &parent, FileName: name, Mode: os.ModeDir})
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
	name = strings.TrimSuffix(name, linkSuffix)

	var destObj Object
	destObj = *&srcFile.Object
	destObj.ID = 0
	destObj.Parent = &parent
	destObj.FileName = name
	err := f.db.Create(&destObj).Error
	return &RegularFile{destObj}, err
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
	RealPath string      `gorm:"default:null;uniqueIndex:idx_real_path_version"`
	Version  int64       `gorm:"default:null;uniqueIndex:idx_real_path_version"`
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
		fileName += linkSuffix
	}

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
	return o.FS.db.Updates(o).Error
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
	opts := rest.Opts{
		Method:       http.MethodGet,
		Path:         r.RealPath,
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
	if r.Mode&os.ModeSymlink > 0 {
		return ioutil.NopCloser(strings.NewReader(r.RealPath)), nil
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
	return r.FS.db.Updates(&r.Object).Error
}

// Remove decreases the version reference count by one,
// which is the opposite action of Copy.
func (r *RegularFile) Remove(ctx context.Context) (err error) {
	// check object exists
	if _, err = r.FS.NewObject(ctx, r.Remote()); err != nil {
		return err
	}

	if err = r.unlink(ctx); err != nil {
		return err
	}
	return r.FS.db.Delete(&r.Object, &r).Error
}

// unlink checks whether the version reference count is no more than one.
// If so, remove this version from the object storage; otherwise do nothing.
func (r *RegularFile) unlink(ctx context.Context) (err error) {
	if r.FileSize <= 0 || r.Mode&os.ModeSymlink > 0 {
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
			Path:       r.RealPath,
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
		Where(&Object{FS: o.FS, RealPath: o.RealPath, Version: o.Version}).
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
	MD5       string `hash:"md5" json:"fileMd5" header:"ETag" gorm:"default:null"`
	SHA1      string `hash:"sha1" json:"fileSha1" gorm:"default:null"`
	CRC64ECMA string `hash:"crc64ecma" header:"x-oss-hash-crc64ecma" gorm:"default:null"`
	SHA256    string `hash:"sha256" json:"fileSha256" gorm:"default:null"`
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
	_ fs.ListRer      = &Fs{}
	_ fs.PutStreamer  = &Fs{}
	_ fs.PublicLinker = &Fs{}
	_ fs.Object       = &RegularFile{}
	_ fs.MimeTyper    = &Object{}
)
