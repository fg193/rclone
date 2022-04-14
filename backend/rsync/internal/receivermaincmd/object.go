package receivermaincmd

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
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
	return nil, fmt.Errorf("Open failed: %w", err)
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
