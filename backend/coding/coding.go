// Package coding provides an interface to Coding Artifact Storage
package coding

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/walk"
	"github.com/rclone/rclone/lib/atexit"
	"github.com/rclone/rclone/lib/bucket"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/pool"
	"github.com/rclone/rclone/lib/readers"
	"github.com/rclone/rclone/lib/rest"
	"golang.org/x/sync/errgroup"
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "coding",
		Description: "Coding Generic Artifact Storage, backed by Tencent COS",
		NewFs:       NewFs,
		CommandHelp: commandHelp,
		Options: []fs.Option{{
			Name: "token",
			Help: `Coding personal token.

Generate or manage these hexadecimal secrets at
https://{team}.coding.net/user/account/setting/tokens.`,
		}, {
			Name: "upload_cutoff",
			Help: `Cutoff for switching to chunked upload.

Any files larger than this will be uploaded in chunks of chunk_size.
The minimum is 0 and the maximum is 5 GiB.`,
			Default:  defaultUploadCutoff,
			Advanced: true,
		}, {
			Name: "chunk_size",
			Help: `Chunk size to use for uploading.

When uploading files larger than upload_cutoff or files with unknown
size (e.g. from "rclone rcat" or uploaded with "rclone mount" or google
photos or google docs) they will be uploaded as multipart uploads
using this chunk size.

Note that "--s3-upload-concurrency" chunks of this size are buffered
in memory per transfer.

If you are transferring large files over high-speed links and you have
enough memory, then increasing this will speed up the transfers.

Rclone will automatically increase the chunk size when uploading a
large file of known size to stay below the 10,000 chunks limit.

Files of unknown size are uploaded with the configured
chunk_size. Since the default chunk size is 5 MiB and there can be at
most 10,000 chunks, this means that by default the maximum size of
a file you can stream upload is 48 GiB.  If you wish to stream upload
larger files then you will need to increase chunk_size.`,
			Default:  minChunkSize,
			Advanced: true,
		}, {
			Name: "max_upload_parts",
			Help: `Maximum number of parts in a multipart upload.

This option defines the maximum number of multipart chunks to use
when doing a multipart upload.

This can be useful if a service does not support the AWS S3
specification of 10,000 chunks.

Rclone will automatically increase the chunk size when uploading a
large file of a known size to stay below this number of chunks limit.
`,
			Default:  maxUploadParts,
			Advanced: true,
		}, {
			Name: "disable_checksum",
			Help: `Don't store MD5 checksum with object metadata.

Normally rclone will calculate the MD5 checksum of the input before
uploading it so it can add it to metadata on the object. This is great
for data integrity checking but can cause long delays for large files
to start uploading.`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "upload_concurrency",
			Help: `Concurrency for multipart uploads.

This is the number of chunks of the same file that are uploaded
concurrently.

If you are uploading small numbers of large files over high-speed links
and these uploads do not fully utilize your bandwidth, then increasing
this may help to speed up the transfers.`,
			Default:  4,
			Advanced: true,
		}, {
			Name: "force_path_style",
			Help: `If true use path style access if false use virtual hosted style.

If this is true (the default) then rclone will use path style access,
if false then rclone will use virtual path style. See [the AWS S3
docs](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro)
for more info.

Some providers (e.g. AWS, Aliyun OSS, Netease COS, or Tencent COS) require this set to
false - rclone will do this automatically based on the provider
setting.`,
			Default:  true,
			Advanced: true,
		}, {
			Name:     "leave_parts_on_error",
			Provider: "AWS",
			Help: `If true avoid calling abort upload on a failure, leaving all successfully uploaded parts on S3 for manual recovery.

It should be set to true for resuming uploads across different sessions.

WARNING: Storing parts of an incomplete multipart upload counts towards space usage on S3 and will add additional costs if not cleaned up.
`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "list_chunk",
			Help: `Size of listing chunk (response list for each ListObject S3 request).

This option is also known as "MaxKeys", "max-items", or "page-size" from the AWS S3 specification.
Most services truncate the response list to 1000 objects even if requested more than that.
In AWS S3 this is a global maximum and cannot be changed, see [AWS S3](https://docs.aws.amazon.com/cli/latest/reference/s3/ls.html).
In Ceph, this can be increased with the "rgw list buckets max chunk" option.
`,
			Default:  1000,
			Advanced: true,
		}, {
			Name: "list_version",
			Help: `Version of ListObjects to use: 1,2 or 0 for auto.

When S3 originally launched it only provided the ListObjects call to
enumerate objects in a bucket.

However in May 2016 the ListObjectsV2 call was introduced. This is
much higher performance and should be used if at all possible.

If set to the default, 0, rclone will guess according to the provider
set which list objects method to call. If it guesses wrong, then it
may be set manually here.
`,
			Default:  0,
			Advanced: true,
		}, {
			Name: "no_check_bucket",
			Help: `If set, don't attempt to check the bucket exists or create it.

This can be useful when trying to minimise the number of transactions
rclone does if you know the bucket exists already.

It can also be needed if the user you are using does not have bucket
creation permissions. Before v1.52.0 this would have passed silently
due to a bug.
`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "no_head",
			Help: `If set, don't HEAD uploaded objects to check integrity.

This can be useful when trying to minimise the number of transactions
rclone does.

Setting it means that if rclone receives a 200 OK message after
uploading an object with PUT then it will assume that it got uploaded
properly.

In particular it will assume:

- the metadata, including modtime, storage class and content type was as uploaded
- the size was as uploaded

It reads the following items from the response for a single part PUT:

- the MD5SUM
- The uploaded date

For multipart uploads these items aren't read.

If an source object of unknown length is uploaded then rclone **will** do a
HEAD request.

Setting this flag increases the chance for undetected upload failures,
in particular an incorrect size, so it isn't recommended for normal
operation. In practice the chance of an undetected upload failure is
very small even with this flag.
`,
			Default:  false,
			Advanced: true,
		}, {
			Name:     "no_head_object",
			Help:     `If set, do not do HEAD before GET when getting objects.`,
			Default:  false,
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			// Any UTF-8 character is valid in a key, however it can't handle
			// invalid UTF-8 and / have a special meaning.
			//
			// The SDK can't seem to handle uploading files called '.'
			//
			// FIXME would be nice to add
			// - initial / encoding
			// - doubled / encoding
			// - trailing / encoding
			// so that AWS keys are always valid file names
			Default: encoder.EncodeInvalidUtf8 |
				encoder.EncodeSlash |
				encoder.EncodeDot,
		}, {
			Name:     "memory_pool_flush_time",
			Default:  memoryPoolFlushTime,
			Advanced: true,
			Help: `How often internal memory buffer pools will be flushed.

Uploads which requires additional buffers (f.e multipart) will use memory pool for allocations.
This option controls how often unused buffers will be removed from the pool.`,
		}, {
			Name:     "memory_pool_use_mmap",
			Default:  memoryPoolUseMmap,
			Advanced: true,
			Help:     `Whether to use mmap buffers in internal memory pool.`,
		}, {
			Name:     "disable_http2",
			Default:  false,
			Advanced: true,
			Help: `Disable usage of http2 for S3 backends.

There is currently an unsolved issue with the s3 (specifically minio) backend
and HTTP/2.  HTTP/2 is enabled by default for the s3 backend but can be
disabled here.  When the issue is solved this flag will be removed.

See: https://github.com/rclone/rclone/issues/4673, https://github.com/rclone/rclone/issues/3631

`,
		}, {
			Name: "download_url",
			Help: `Custom endpoint for downloads.
This is usually set to a CloudFront CDN URL as AWS S3 offers
cheaper egress for data downloaded through the CloudFront network.`,
			Advanced: true,
		},
		}})
}

const (
	metaMTime   = "mtime" // the meta key to store mtime in
	metaMD5Hash = "md5"   // the meta key to store MD5 hash in

	maxUploadParts      = 10000 // maximum allowed number of parts in a multi-part upload
	minChunkSize        = fs.SizeSuffix(1024 * 1024 * 5)
	defaultUploadCutoff = fs.SizeSuffix(200 * 1024 * 1024)
	maxUploadCutoff     = fs.SizeSuffix(5 * 1024 * 1024 * 1024)
	minSleep            = 10 * time.Millisecond // In case of error, start at 10ms sleep.

	memoryPoolFlushTime = fs.Duration(time.Minute) // flush the cached buffers after this long
	memoryPoolUseMmap   = false
	maxExpireDuration   = fs.Duration(7 * 24 * time.Hour) // max expiry is 1 week
)

// Options defines the configuration for this backend
type Options struct {
	Token                 string               `config:"token"`
	UploadCutoff          fs.SizeSuffix        `config:"upload_cutoff"`
	ChunkSize             fs.SizeSuffix        `config:"chunk_size"`
	MaxUploadParts        int64                `config:"max_upload_parts"`
	DisableChecksum       bool                 `config:"disable_checksum"`
	UploadConcurrency     int                  `config:"upload_concurrency"`
	ForcePathStyle        bool                 `config:"force_path_style"`
	AccelerateEndpoint___ bool                 `config:"use_accelerate_endpoint"`
	LeavePartsOnError     bool                 `config:"leave_parts_on_error"`
	ListChunk             int                  `config:"list_chunk"`
	ListVersion           int                  `config:"list_version"`
	NoCheckBucket         bool                 `config:"no_check_bucket"`
	NoHead                bool                 `config:"no_head"`
	NoHeadObject          bool                 `config:"no_head_object"`
	Enc                   encoder.MultiEncoder `config:"encoding"`
	MemoryPoolFlushTime   fs.Duration          `config:"memory_pool_flush_time"`
	MemoryPoolUseMmap     bool                 `config:"memory_pool_use_mmap"`
	DownloadURL           string               `config:"download_url"`
}

// Fs represents a remote s3 server
type Fs struct {
	name          string         // the name of the remote
	root          string         // root of the bucket - ignore all objects above this
	opt           Options        // parsed options
	ci            *fs.ConfigInfo // global config
	features      *fs.Features   // optional features
	project       uintptr        // Coding project ID
	rootBucket    string         // bucket part of root (if any)
	rootDirectory string         // directory part of root (if any)
	cache         *bucket.Cache  // cache for bucket creation status
	pacer         *fs.Pacer      // To pace the API calls
	srvRest       *rest.Client   // the rest connection to the server
	pool          *pool.Pool     // memory pool
}

// Object describes a s3 object
type Object struct {
	// Will definitely have everything but meta which may be nil
	//
	// List will read everything but meta & mimeType - to fill
	// that in you need to call readMetaData
	fs           *Fs                // what this object is part of
	remote       string             // The remote path
	md5          string             // md5sum of the object
	bytes        int64              // size of the object
	lastModified time.Time          // Last modified
	properties   map[string]*string // The object metadata if known - may be nil
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	if f.rootBucket == "" {
		return fmt.Sprintf("Coding [/]")
	}
	if f.rootDirectory == "" {
		return fmt.Sprintf("Coding [%s]", f.rootBucket)
	}
	return fmt.Sprintf("Coding [%s] %s", f.rootBucket, f.rootDirectory)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// retryErrorCodes is a slice of error codes that we will retry
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
var retryErrorCodes = []int{
	429, // Too Many Requests
	500, // Internal Server Error - "We encountered an internal error. Please try again."
	503, // Service Unavailable/Slow Down - "Reduce your request rate"
}

//S3 is pretty resilient, and the built in retry handling is probably sufficient
// as it should notice closed connections and timeouts which are the most likely
// sort of failure modes
func (f *Fs) shouldRetry(ctx context.Context, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	// If this is an awserr object, try and extract more useful information to determine if we should retry
	if awsError, ok := err.(awserr.Error); ok {
		// Simple case, check the original embedded error in case it's generically retryable
		if fserrors.ShouldRetry(awsError.OrigErr()) {
			return true, err
		}
		// Failing that, if it's a RequestFailure it's probably got an http status code we can check
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			// 301 if wrong region for bucket - can only update if running from a bucket
			for _, e := range retryErrorCodes {
				if reqErr.StatusCode() == e {
					return true, err
				}
			}
		}
	}
	// Ok, not an awserr, check for generic failure conditions
	return fserrors.ShouldRetry(err), err
}

// parsePath parses a remote 'url'
func parsePath(path string) (root string) {
	root = strings.Trim(path, "/")
	return
}

// split returns bucket and bucketPath from the rootRelativePath
// relative to f.root
func (f *Fs) split(rootRelativePath string) (bucketName, bucketPath string) {
	bucketName, bucketPath = bucket.Split(path.Join(f.root, rootRelativePath))
	return f.opt.Enc.FromStandardName(bucketName), f.opt.Enc.FromStandardPath(bucketPath)
}

// split returns bucket and bucketPath from the object
func (o *Object) split() (bucket, bucketPath string) {
	return o.fs.split(o.remote)
}

func checkUploadChunkSize(cs fs.SizeSuffix) error {
	if cs < minChunkSize {
		return fmt.Errorf("%s is less than %s", cs, minChunkSize)
	}
	return nil
}

func (f *Fs) setUploadChunkSize(cs fs.SizeSuffix) (old fs.SizeSuffix, err error) {
	err = checkUploadChunkSize(cs)
	if err == nil {
		old, f.opt.ChunkSize = f.opt.ChunkSize, cs
	}
	return
}

func checkUploadCutoff(cs fs.SizeSuffix) error {
	if cs > maxUploadCutoff {
		return fmt.Errorf("%s is greater than %s", cs, maxUploadCutoff)
	}
	return nil
}

func (f *Fs) setUploadCutoff(cs fs.SizeSuffix) (old fs.SizeSuffix, err error) {
	err = checkUploadCutoff(cs)
	if err == nil {
		old, f.opt.UploadCutoff = f.opt.UploadCutoff, cs
	}
	return
}

// setRoot changes the root of the Fs
func (f *Fs) setRoot(root string) {
	f.root = parsePath(root)
	f.rootBucket, f.rootDirectory = bucket.Split(f.root)
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	var opt Options
	err := configstruct.Set(m, &opt)
	if err != nil {
		return nil, err
	}
	err = checkUploadChunkSize(opt.ChunkSize)
	if err != nil {
		return nil, fmt.Errorf("s3: chunk size: %w", err)
	}
	err = checkUploadCutoff(opt.UploadCutoff)
	if err != nil {
		return nil, fmt.Errorf("s3: upload cutoff: %w", err)
	}
	ci := fs.GetConfig(ctx)

	pc := fs.NewPacer(ctx, pacer.NewS3(pacer.MinSleep(minSleep)))
	// Set pacer retries to 2 (1 try and 1 retry) because we are
	// relying on SDK retry mechanism, but we allow 2 attempts to
	// retry directory listings after XMLSyntaxError
	pc.SetRetries(2)

	f := &Fs{
		name:    name,
		opt:     opt,
		ci:      ci,
		pacer:   pc,
		cache:   bucket.NewCache(),
		srvRest: rest.NewClient(fshttp.NewClient(ctx)),
		pool: pool.New(
			time.Duration(opt.MemoryPoolFlushTime),
			int(opt.ChunkSize),
			opt.UploadConcurrency*ci.Transfers,
			opt.MemoryPoolUseMmap,
		),
	}
	f.srvRest.SetRoot("https://e.coding.net/open-api")
	f.srvRest.SetHeader("Authorization", "token "+opt.Token)
	f.setRoot(root)
	f.features = (&fs.Features{
		BucketBased:       true,
		BucketBasedRootOK: true,
		SlowModTime:       true,
		SlowHash:          true,
	}).Fill(ctx, f)

	// Find the first project in the team
	req := DescribeCodingProjectsRequest{
		Page: Page{PageNumber: 1, PageSize: f.opt.ListChunk},
	}
	resp := DescribeCodingProjectsResponse{}
	if _, err = f.call(ctx, &req, &resp); err != nil {
		return nil, err
	}
	if len(resp.Data.ProjectList) == 0 {
		return nil, fmt.Errorf("cannot find project")
	}
	f.project = resp.Data.ProjectList[0].Id

	if f.rootBucket != "" && f.rootDirectory != "" && !opt.NoHeadObject && !strings.HasSuffix(root, "/") {
		// Check to see if the (bucket,directory) is actually an existing file
		oldRoot := f.root
		newRoot, leaf := path.Split(oldRoot)
		f.setRoot(newRoot)
		_, err := f.NewObject(ctx, leaf)
		if err != nil {
			// File doesn't exist or is a directory so return old f
			f.setRoot(oldRoot)
			return f, nil
		}
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}
	return f, nil
}

// Return an Object from a path
//
//If it can't be found it returns the error ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *ArtifactPackageBean) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	if info != nil {
		// Set info but not meta
		if info.CreatedAt == 0 {
			fs.Logf(o, "Failed to read last modified")
			o.lastModified = time.Now()
		} else {
			o.lastModified = info.CreatedAt.Into()
		}
		// o.setMD5FromEtag(aws.StringValue(info.ETag))
		// o.bytes = aws.Int64Value(info.Size)
	} else if !o.fs.opt.NoHeadObject {
		err := o.readMetaData(ctx) // reads info and meta, returning an error
		if err != nil {
			return nil, err
		}
	}
	return o, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObjectWithInfo(ctx, remote, nil)
}

// listFn is called from list to handle an object.
type listFn func(remote string, object *ArtifactPackageBean, isDirectory bool) error

// list lists the objects into the function supplied from
// the bucket and directory supplied.  The remote has prefix
// removed from it and if addBucket is set then it adds the
// bucket to the start.
//
// Set recurse to read sub directories
func (f *Fs) list(ctx context.Context, bucket, directory, prefix string, addBucket bool, recurse bool, fn listFn) error {
	if prefix != "" {
		prefix += "/"
	}
	if directory != "" {
		directory += "/"
	}
	delimiter := ""
	if !recurse {
		delimiter = "/"
	}
	_ = delimiter
	// URL encode the listings so we can use control characters in object names
	// See: https://github.com/aws/aws-sdk-go/issues/1914
	//
	// However this doesn't work perfectly under Ceph (and hence DigitalOcean/Dreamhost) because
	// it doesn't encode CommonPrefixes.
	// See: https://tracker.ceph.com/issues/41870
	//
	// This does not work under IBM COS also: See https://github.com/rclone/rclone/issues/3345
	// though maybe it does on some versions.
	//
	// This does work with minio but was only added relatively recently
	// https://github.com/minio/minio/pull/7265
	//
	// So we enable only on providers we know supports it properly, all others can retry when a
	// XML Syntax error is detected.
	for page := 1; ; page++ {
		// FIXME need to implement ALL loop
		req := DescribeArtifactPackageListRequest{
			ProjectId:     f.project,
			Repository:    bucket,
			PackagePrefix: directory,
			Page:          Page{PageNumber: page, PageSize: f.opt.ListChunk},
		}
		resp := DescribeArtifactPackageListResponse{}
		if err := f.pacer.Call(func() (bool, error) {
			_, err := f.call(ctx, &req, &resp)
			return f.shouldRetry(ctx, err)
		}); err != nil {
			return err
		}
		if !recurse {
			var commonPrefixes []string
			for _, remote := range commonPrefixes {
				remote = f.opt.Enc.ToStandardPath(remote)
				if !strings.HasPrefix(remote, prefix) {
					fs.Logf(f, "Odd name received %q", remote)
					continue
				}
				remote = remote[len(prefix):]
				if addBucket {
					remote = path.Join(bucket, remote)
				}
				if strings.HasSuffix(remote, "/") {
					remote = remote[:len(remote)-1]
				}
				if err := fn(remote, &ArtifactPackageBean{Name: remote}, true); err != nil {
					return err
				}
			}
		}

		for _, object := range resp.Data.InstanceSet {
			remote := f.opt.Enc.ToStandardPath(object.Name)
			if !strings.HasPrefix(remote, prefix) {
				fs.Logf(f, "Odd name received %q", remote)
				continue
			}
			remote = remote[len(prefix):]
			isDirectory := remote == "" || strings.HasSuffix(remote, "/")
			if addBucket {
				remote = path.Join(bucket, remote)
			}
			// is this a directory marker?
			if isDirectory { // && object.Size != nil && *object.Size == 0
				continue // skip directory marker
			}
			if err := fn(remote, &object, false); err != nil {
				return err
			}
		}

		if len(resp.Data.InstanceSet) < f.opt.ListChunk {
			return nil
		}
	}
}

// Convert a list item into a DirEntry
func (f *Fs) itemToDirEntry(ctx context.Context, remote string, object *ArtifactPackageBean, isDirectory bool) (fs.DirEntry, error) {
	if isDirectory {
		// size := int64(0)
		// if object.Size != nil {
		// 	size = *object.Size
		// }
		d := fs.NewDir(remote, time.Time{}) // .SetSize(size)
		return d, nil
	}
	o, err := f.newObjectWithInfo(ctx, remote, object)
	if err != nil {
		return nil, err
	}
	return o, nil
}

// listDir lists files and directories to out
func (f *Fs) listDir(ctx context.Context, bucket, directory, prefix string, addBucket bool) (entries fs.DirEntries, err error) {
	// List the objects and directories
	err = f.list(ctx, bucket, directory, prefix, addBucket, false, func(remote string, object *ArtifactPackageBean, isDirectory bool) error {
		entry, err := f.itemToDirEntry(ctx, remote, object, isDirectory)
		if err != nil {
			return err
		}
		if entry != nil {
			entries = append(entries, entry)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// bucket must be present if listing succeeded
	f.cache.MarkOK(bucket)
	return entries, nil
}

// listBuckets lists the buckets to out
func (f *Fs) listBuckets(ctx context.Context) (entries fs.DirEntries, err error) {
	req := DescribeArtifactRepositoryListRequest{
		ProjectId: f.project,
		Type:      RepositoryTypeGeneric,
		Page:      Page{PageNumber: 1, PageSize: f.opt.ListChunk},
	}
	resp := DescribeArtifactRepositoryListResponse{}
	err = f.pacer.Call(func() (bool, error) {
		var err error
		_, err = f.call(ctx, &req, &resp)
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		return nil, err
	}
	for _, bucket := range resp.Data.InstanceSet {
		f.cache.MarkOK(bucket.Name)
		d := fs.NewDir(bucket.Name, bucket.CreatedAt.Into())
		entries = append(entries, d)
	}
	return entries, nil
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
	bucket, directory := f.split(dir)
	if bucket == "" {
		if directory != "" {
			return nil, fs.ErrorListBucketRequired
		}
		return f.listBuckets(ctx)
	}
	return f.listDir(ctx, bucket, directory, f.rootDirectory, f.rootBucket == "")
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
	bucket, directory := f.split(dir)
	list := walk.NewListRHelper(callback)
	listR := func(bucket, directory, prefix string, addBucket bool) error {
		return f.list(ctx, bucket, directory, prefix, addBucket, true, func(remote string, object *ArtifactPackageBean, isDirectory bool) error {
			entry, err := f.itemToDirEntry(ctx, remote, object, isDirectory)
			if err != nil {
				return err
			}
			return list.Add(entry)
		})
	}
	if bucket == "" {
		entries, err := f.listBuckets(ctx)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			err = list.Add(entry)
			if err != nil {
				return err
			}
			bucket := entry.Remote()
			err = listR(bucket, "", f.rootDirectory, true)
			if err != nil {
				return err
			}
			// bucket must be present if listing succeeded
			f.cache.MarkOK(bucket)
		}
	} else {
		err = listR(bucket, directory, f.rootDirectory, f.rootBucket == "")
		if err != nil {
			return err
		}
		// bucket must be present if listing succeeded
		f.cache.MarkOK(bucket)
	}
	return list.Flush()
}

// Put the Object into the bucket
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// Temporary Object under construction
	fs := &Object{
		fs:     f,
		remote: src.Remote(),
	}
	return fs, fs.Update(ctx, in, src, options...)
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

// Check if the bucket exists
//
// NB this can return incorrect results if called immediately after bucket deletion
func (f *Fs) bucketExists(ctx context.Context, bucket string) (bool, error) {
	req := DescribeArtifactRepositoryListRequest{}
	resp := DescribeArtifactRepositoryListResponse{}
	err := f.pacer.Call(func() (bool, error) {
		_, err := f.call(ctx, &req, &resp)
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		return false, err
	}
	for _, repo := range resp.Data.InstanceSet {
		if repo.Name == bucket {
			return true, nil
		}
	}
	return false, nil
}

// Mkdir creates the bucket if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	bucket, _ := f.split(dir)
	return f.makeBucket(ctx, bucket)
}

// makeBucket creates the bucket if it doesn't exist
func (f *Fs) makeBucket(ctx context.Context, bucket string) error {
	if f.opt.NoCheckBucket {
		return nil
	}
	return f.cache.Create(bucket, func() error {
		req := CreateArtifactRepositoryRequest{
			ProjectId:      f.project,
			RepositoryName: bucket,
			Type:           RepositoryTypeGeneric,
		}
		resp := CreateArtifactRepositoryResponse{}
		err := f.pacer.Call(func() (bool, error) {
			_, err := f.call(ctx, &req, &resp)
			return f.shouldRetry(ctx, err)
		})
		if err == nil {
			fs.Infof(f, "Bucket %q created", bucket)
		}
		return err
	}, func() (bool, error) {
		return f.bucketExists(ctx, bucket)
	})
}

// Rmdir deletes the bucket if the fs is at the root
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	bucket, directory := f.split(dir)
	if bucket == "" || directory != "" {
		return nil
	}
	return fs.ErrorCantPurge
}

// Precision of the remote
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// pathEscape escapes s as for a URL path.  It uses rest.URLPathEscape
// but also escapes '+' for S3 and Digital Ocean spaces compatibility
func pathEscape(s string) string {
	return strings.Replace(rest.URLPathEscape(s), "+", "%2B", -1)
}

func calculateRange(partSize, partIndex, numParts, totalSize int64) string {
	start := partIndex * partSize
	var ends string
	if partIndex == numParts-1 {
		if totalSize >= 1 {
			ends = strconv.FormatInt(totalSize-1, 10)
		}
	} else {
		ends = strconv.FormatInt(start+partSize-1, 10)
	}
	return fmt.Sprintf("bytes=%v-%v", start, ends)
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.MD5)
}

func (f *Fs) getMemoryPool(size int64) *pool.Pool {
	if size == int64(f.opt.ChunkSize) {
		return f.pool
	}

	return pool.New(
		time.Duration(f.opt.MemoryPoolFlushTime),
		int(size),
		f.opt.UploadConcurrency*f.ci.Transfers,
		f.opt.MemoryPoolUseMmap,
	)
}

// PublicLink generates a public link to the remote path (usually readable by anyone)
func (f *Fs) PublicLink(ctx context.Context, remote string, expire fs.Duration, unlink bool) (link string, err error) {
	if strings.HasSuffix(remote, "/") {
		return "", fs.ErrorCantShareDirectories
	}
	if _, err := f.NewObject(ctx, remote); err != nil {
		return "", err
	}
	if expire > maxExpireDuration {
		fs.Logf(f, "Public Link: Reducing expiry to %v as %v is greater than the max time allowed", maxExpireDuration, expire)
		expire = maxExpireDuration
	}
	bucket, bucketPath := f.split(remote)

	req := DescribeArtifactFileDownloadUrlRequest{
		ProjectId:      f.project,
		Repository:     bucket,
		Package:        bucketPath,
		PackageVersion: LatestVersion,
		Timeout:        Timestamp(time.Duration(expire).Seconds()),
	}
	resp := DescribeArtifactFileDownloadUrlResponse{}
	_, err = f.call(ctx, &req, &resp)
	return resp.Url, err
}

var commandHelp = []fs.CommandHelp{{
	Name:  "list-multipart-uploads",
	Short: "List the unfinished multipart uploads",
	Long: `This command lists the unfinished multipart uploads in JSON format.

    rclone backend list-multipart s3:bucket/path/to/object

It returns a dictionary of buckets with values as lists of unfinished
multipart uploads.

You can call it with no bucket in which case it lists all bucket, with
a bucket or with a bucket and path.

    {
      "rclone": [
        {
          "Initiated": "2020-06-26T14:20:36Z",
          "Initiator": {
            "DisplayName": "XXX",
            "ID": "arn:aws:iam::XXX:user/XXX"
          },
          "Key": "KEY",
          "Owner": {
            "DisplayName": null,
            "ID": "XXX"
          },
          "UploadId": "XXX"
        }
      ],
      "rclone-1000files": [],
      "rclone-dst": []
    }

`,
}, {
	Name:  "cleanup",
	Short: "Remove unfinished multipart uploads.",
	Long: `This command removes unfinished multipart uploads of age greater than
max-age which defaults to 24 hours.

Note that you can use -i/--dry-run with this command to see what it
would do.

    rclone backend cleanup s3:bucket/path/to/object
    rclone backend cleanup -o max-age=7w s3:bucket/path/to/object

Durations are parsed as per the rest of rclone, 2h, 7d, 7w etc.
`,
	Opts: map[string]string{
		"max-age": "Max age of upload to delete",
	},
}}

// Command the backend to run a named command
//
// The command run is name
// args may be used to read arguments from
// opts may be used to read optional arguments from
//
// The result should be capable of being JSON encoded
// If it is a string or a []string it will be shown to the user
// otherwise it will be JSON encoded and shown to the user like that
func (f *Fs) Command(ctx context.Context, name string, arg []string, opt map[string]string) (out interface{}, err error) {
	switch name {
	case "cleanup":
		maxAge := 24 * time.Hour
		if opt["max-age"] != "" {
			maxAge, err = fs.ParseDuration(opt["max-age"])
			if err != nil {
				return nil, fmt.Errorf("bad max-age: %w", err)
			}
		}
		return nil, f.cleanUp(ctx, maxAge)
	default:
		return nil, fs.ErrorCommandNotFound
	}
}

// CleanUp removes all pending multipart uploads
func (f *Fs) cleanUp(ctx context.Context, maxAge time.Duration) (err error) {
	// fs.Debugf(f, "ignoring %s", what)
	return err
}

// CleanUp removes all pending multipart uploads older than 24 hours
func (f *Fs) CleanUp(ctx context.Context) (err error) {
	return f.cleanUp(ctx, 24*time.Hour)
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

var matchMd5 = regexp.MustCompile(`^[0-9a-f]{32}$`)

// Set the MD5 from the etag
func (o *Object) setMD5FromEtag(etag string) {
	if etag == "" {
		o.md5 = ""
		return
	}
	hash := strings.Trim(strings.ToLower(etag), `" `)
	// Check the etag is a valid md5sum
	if !matchMd5.MatchString(hash) {
		o.md5 = ""
		return
	}
	o.md5 = hash
}

// Hash returns the Md5sum of an object returning a lowercase hex string
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	// If we haven't got an MD5, then check the metadata
	if o.md5 == "" {
		err := o.readMetaData(ctx)
		if err != nil {
			return "", err
		}
	}
	return o.md5, nil
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.bytes
}

// readMetaData gets the metadata if it hasn't already been fetched
//
// it also sets the info
func (o *Object) readMetaData(ctx context.Context) error {
	if o.properties != nil {
		return nil
	}
	bucket, bucketPath := o.split()
	req := DescribeArtifactVersionListRequest{
		ProjectId:  o.fs.project,
		Repository: bucket,
		Package:    bucketPath,
	}
	resp := DescribeArtifactVersionListResponse{}
	if err := o.fs.pacer.Call(func() (bool, error) {
		var err error
		_, err = o.fs.call(ctx, &req, &resp)
		return o.fs.shouldRetry(ctx, err)
	}); err != nil {
		return err
	}
	// TODO
	if 0 == http.StatusNotFound || len(resp.Data.InstanceSet) == 0 {
		return fs.ErrorObjectNotFound
	}
	o.fs.cache.MarkOK(bucket)
	head := resp.Data.InstanceSet[0]
	md5sum := ""
	if strings.HasPrefix(head.Hash, metaMD5Hash) {
		md5sum = head.Hash[len(metaMD5Hash):]
	}
	sizeInBytes := int64(head.Size * (1 << 20))
	lastModified := head.CreatedAt.Into()
	o.setMetaData(md5sum, &sizeInBytes, &lastModified, nil)
	return nil
}

func (o *Object) setMetaData(etag string, contentLength *int64, lastModified *time.Time, meta map[string]*string) {
	// Ignore missing Content-Length assuming it is 0
	// Some versions of ceph do this due their apache proxies
	if contentLength != nil {
		o.bytes = *contentLength
	}
	o.setMD5FromEtag(etag)
	o.properties = meta
	if o.properties == nil {
		o.properties = map[string]*string{}
	}
	// Read MD5 from metadata if present
	if md5sumBase64, ok := o.properties[metaMD5Hash]; ok {
		md5sumBytes, err := base64.StdEncoding.DecodeString(*md5sumBase64)
		if err != nil {
			fs.Debugf(o, "Failed to read md5sum from metadata %q: %v", *md5sumBase64, err)
		} else if len(md5sumBytes) != 16 {
			fs.Debugf(o, "Failed to read md5sum from metadata %q: wrong length", *md5sumBase64)
		} else {
			o.md5 = hex.EncodeToString(md5sumBytes)
		}
	}
	if lastModified == nil {
		o.lastModified = time.Now()
		fs.Logf(o, "Failed to read last modified")
	} else {
		o.lastModified = *lastModified
	}
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime(ctx context.Context) time.Time {
	if o.fs.ci.UseServerModTime {
		return o.lastModified
	}
	err := o.readMetaData(ctx)
	if err != nil {
		fs.Logf(o, "Failed to read metadata: %v", err)
		return time.Now()
	}
	// read mtime out of metadata if available
	d, ok := o.properties[metaMTime]
	if !ok || d == nil {
		// fs.Debugf(o, "No metadata")
		return o.lastModified
	}
	modTime, err := strconv.ParseInt(*d, 10, 64)
	if err != nil {
		fs.Logf(o, "Failed to read mtime from object: %v", err)
		return o.lastModified
	}
	return Timestamp(modTime).Into()
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

// Storable returns a boolean indicating if this object is storable
func (o *Object) Storable() bool {
	return true
}

func (o *Object) downloadFromURL(ctx context.Context, url string, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	var resp *http.Response
	opts := rest.Opts{
		Method:  "GET",
		RootURL: url,
		Options: options,
	}
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srvRest.Call(ctx, &opts)
		return o.fs.shouldRetry(ctx, err)
	})
	if err != nil {
		return nil, err
	}

	contentLength := &resp.ContentLength
	if resp.Header.Get("Content-Range") != "" {
		var contentRange = resp.Header.Get("Content-Range")
		slash := strings.IndexRune(contentRange, '/')
		if slash >= 0 {
			i, err := strconv.ParseInt(contentRange[slash+1:], 10, 64)
			if err == nil {
				contentLength = &i
			} else {
				fs.Debugf(o, "Failed to find parse integer from in %q: %v", contentRange, err)
			}
		} else {
			fs.Debugf(o, "Failed to find length in %q", contentRange)
		}
	}

	lastModified, err := time.Parse(time.RFC1123, resp.Header.Get("Last-Modified"))
	if err != nil {
		fs.Debugf(o, "Failed to parse last modified from string %s, %v", resp.Header.Get("Last-Modified"), err)
	}

	metaData := make(map[string]*string)
	for key, value := range resp.Header {
		if strings.HasPrefix(key, "x-amz-meta") {
			metaKey := strings.TrimPrefix(key, "x-amz-meta-")
			metaData[strings.Title(metaKey)] = &value[0]
		}
	}

	etag := resp.Header.Get("Etag")

	o.setMetaData(etag, contentLength, &lastModified, metaData)
	return resp.Body, err
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	bucket, bucketPath := o.split()

	if len(o.fs.opt.DownloadURL) > 0 {
		return o.downloadFromURL(ctx, o.fs.opt.DownloadURL+bucketPath, options...)
	}

	req := DescribeArtifactFileDownloadUrlRequest{
		ProjectId:      o.fs.project,
		Repository:     bucket,
		Package:        bucketPath,
		PackageVersion: LatestVersion,
	}
	resp := DescribeArtifactFileDownloadUrlResponse{}
	if _, err = o.fs.call(ctx, &req, &resp); err != nil {
		return nil, err
	}
	return o.downloadFromURL(ctx, resp.Url, options...)
}

var warnStreamUpload sync.Once

func (o *Object) uploadMultipart(ctx context.Context, size int64, in io.Reader) (err error) {
	f := o.fs

	// make concurrency machinery
	concurrency := f.opt.UploadConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	tokens := pacer.NewTokenDispenser(concurrency)

	uploadParts := f.opt.MaxUploadParts
	if uploadParts < 1 {
		uploadParts = 1
	} else if uploadParts > maxUploadParts {
		uploadParts = maxUploadParts
	}

	// calculate size of parts
	partSize := int(f.opt.ChunkSize)

	// size can be -1 here meaning we don't know the size of the incoming file. We use ChunkSize
	// buffers here (default 5 MiB). With a maximum number of parts (10,000) this will be a file of
	// 48 GiB which seems like a not too unreasonable limit.
	if size == -1 {
		warnStreamUpload.Do(func() {
			fs.Logf(f, "Streaming uploads using chunk size %v will have maximum file size of %v",
				f.opt.ChunkSize, fs.SizeSuffix(int64(partSize)*uploadParts))
		})
	} else {
		// Adjust partSize until the number of parts is small enough.
		if size/int64(partSize) >= uploadParts {
			// Calculate partition size rounded up to the nearest MiB
			partSize = int((((size / uploadParts) >> 20) + 1) << 20)
		}
	}

	memPool := f.getMemoryPool(int64(partSize))
	hash := md5.New()
	if _, err = io.Copy(hash, in); err != nil {
		return
	}
	o.md5 = hex.EncodeToString(hash.Sum(nil))

	req := GetArtifactVersionExistChunksRequest{
		Version:  LatestVersion,
		FileTag:  o.md5,
		FileSize: o.bytes,
	}
	resp := GetArtifactVersionExistChunksResponse{}
	err = f.pacer.Call(func() (bool, error) {
		var err error
		_, err = o.fs.callRest(ctx, o.remote, &req, &resp)
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		return fmt.Errorf("multipart upload failed to initialise: %w", err)
	}
	defer atexit.OnError(&err, func() {
		if o.fs.opt.LeavePartsOnError {
			return
		}
		fs.Debugf(o, "Cancelling multipart upload")
		errCancel := f.pacer.Call(func() (bool, error) {
			// _, err := f.c.AbortMultipartUploadWithContext(context.Background(), &s3.AbortMultipartUploadInput{})
			return f.shouldRetry(ctx, err)
		})
		if errCancel != nil {
			fs.Debugf(o, "Failed to cancel multipart upload: %v", errCancel)
		}
	})()

	var (
		g, gCtx  = errgroup.WithContext(ctx)
		finished = false
		offset   int64
	)

	for partNum := 1; !finished; partNum++ {
		// Get a block of memory from the pool and token which limits concurrency.
		tokens.Get()
		buf := memPool.Get()

		free := func() {
			// return the memory and token
			memPool.Put(buf)
			tokens.Put()
		}

		// Fail fast, in case an errgroup managed function returns an error
		// gCtx is cancelled. There is no point in uploading all the other parts.
		if gCtx.Err() != nil {
			free()
			break
		}

		// Read the chunk
		var n int
		n, err = readers.ReadFill(in, buf) // this can never return 0, nil
		if err == io.EOF {
			if n == 0 && partNum != 1 { // end if no data and if not first chunk
				free()
				break
			}
			finished = true
		} else if err != nil {
			free()
			return fmt.Errorf("multipart upload failed to read source: %w", err)
		}
		buf = buf[:n]

		partNum := partNum
		fs.Debugf(o, "multipart upload starting chunk %d size %v offset %v/%v", partNum, fs.SizeSuffix(n), fs.SizeSuffix(offset), fs.SizeSuffix(size))
		offset += int64(n)
		g.Go(func() (err error) {
			defer free()

			err = f.pacer.Call(func() (bool, error) {
				uploadPartReq := UploadArtifactVersionChunkRequest{
					Version:    LatestVersion,
					UploadId:   resp.Data.UploadId,
					PartNumber: partNum,
					ChunkSize:  int64(len(buf)),
				}
				uploadPartResp := map[string]interface{}{}
				_, err = o.fs.callRest(ctx, o.remote, &uploadPartReq, &uploadPartResp)
				if err != nil {
					if partNum <= concurrency {
						return f.shouldRetry(ctx, err)
					}
					// retry all chunks once have done the first batch
					return true, err
				}
				return false, nil
			})
			if err != nil {
				return fmt.Errorf("multipart upload failed to upload part: %w", err)
			}
			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return err
	}

	err = f.pacer.Call(func() (bool, error) {
		req := MergeArtifactVersionChunksRequest{
			Version:  LatestVersion,
			UploadId: resp.Data.UploadId,
			FileTag:  o.md5,
			FileSize: o.Size(),
		}
		resp := map[string]interface{}{}
		_, err := o.fs.callRest(ctx, o.remote, &req, &resp)
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		return fmt.Errorf("multipart upload failed to finalise: %w", err)
	}
	return nil
}

// Update the Object from in with modTime and size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	bucket, _ := o.split()
	err := o.fs.makeBucket(ctx, bucket)
	if err != nil {
		return err
	}
	modTime := src.ModTime(ctx)
	size := src.Size()

	multipart := size < 0 || size >= int64(o.fs.opt.UploadCutoff)

	// Set the mtime in the meta data
	metadata := map[string]string{
		metaMTime: strconv.FormatInt(modTime.Unix(), 10),
	}

	// read the md5sum if available
	// - for non multipart
	//    - so we can add a ContentMD5
	//    - so we can add the md5sum in the metadata as metaMD5Hash if using SSE/SSE-C
	// - for multipart provided checksums aren't disabled
	//    - so we can add the md5sum in the metadata as metaMD5Hash
	var md5sumBase64 string
	var md5sumHex string
	if !multipart || !o.fs.opt.DisableChecksum {
		md5sumHex, err = src.Hash(ctx, hash.MD5)
		if err == nil && matchMd5.MatchString(md5sumHex) {
			hashBytes, err := hex.DecodeString(md5sumHex)
			if err == nil {
				md5sumBase64 = base64.StdEncoding.EncodeToString(hashBytes)
				if multipart && !o.fs.opt.DisableChecksum {
					// Set the md5sum as metadata on the object if
					// - a multipart upload
					// - the Etag is not an MD5, eg when using SSE/SSE-C
					// provided checksums aren't disabled
					metadata[metaMD5Hash] = md5sumBase64
				}
			}
		}
	}

	var resp *http.Response // response from PUT
	if multipart {
		err = o.uploadMultipart(ctx, size, in)
		if err != nil {
			return err
		}
	} else {

		// Set request to nil if empty so as not to make chunked encoding
		if size == 0 {
			in = nil
		}
		// httpReq.ContentLength = size
		err = o.fs.pacer.CallNoRetry(func() (bool, error) {
			var err error
			_, err = o.fs.callRest(ctx, o.remote, &UploadArtifactVersionRequest{}, nil)
			if err != nil {
				return o.fs.shouldRetry(ctx, err)
			}
			return fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
		})
		if err != nil {
			return err
		}
	}

	// User requested we don't HEAD the object after uploading it
	// so make up the object as best we can assuming it got
	// uploaded properly. If size < 0 then we need to do the HEAD.
	if o.fs.opt.NoHead && size >= 0 {
		o.md5 = md5sumHex
		o.bytes = size
		o.lastModified = time.Now()
		// o.properties = req.Metadata

		// If we have done a single part PUT request then we can read these
		if resp != nil {
			if date, err := http.ParseTime(resp.Header.Get("Date")); err == nil {
				o.lastModified = date
			}
			o.setMD5FromEtag(resp.Header.Get("Etag"))
		}
		return nil
	}

	// Read the metadata from the newly created object
	o.properties = nil // wipe old metadata
	o.readMetaData(ctx)
	return err
}

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.pacer.Call(func() (bool, error) {
		_, err := o.fs.callRest(
			ctx, o.remote, &DeleteArtifactVersionRequest{}, nil)
		return o.fs.shouldRetry(ctx, err)
	})
}

// Check the interfaces are satisfied
var (
	_ fs.Fs          = &Fs{}
	_ fs.PutStreamer = &Fs{}
	_ fs.ListRer     = &Fs{}
	_ fs.Commander   = &Fs{}
	_ fs.CleanUpper  = &Fs{}
	_ fs.Object      = &Object{}
)
