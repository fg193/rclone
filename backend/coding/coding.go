// Package coding provides an interface to Coding Artifact Storage
package coding

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/ncw/swift/v2"
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
	"github.com/rclone/rclone/lib/structs"
	"golang.org/x/sync/errgroup"
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "coding",
		Description: "Coding Artifact Storage, backed by Tencent COS",
		NewFs:       NewFs,
		CommandHelp: commandHelp,
		Options: []fs.Option{{
			Name: fs.ConfigProvider,
			Help: "Choose your S3 provider.",
			// NB if you add a new provider here, then add it in the
			// setQuirks function and set the correct quirks
			Examples: []fs.OptionExample{{
				Value: "AWS",
				Help:  "Amazon Web Services (AWS) S3",
			}, {
				Value: "Alibaba",
				Help:  "Alibaba Cloud Object Storage System (OSS) formerly Aliyun",
			}, {
				Value: "Ceph",
				Help:  "Ceph Object Storage",
			}, {
				Value: "DigitalOcean",
				Help:  "Digital Ocean Spaces",
			}, {
				Value: "Dreamhost",
				Help:  "Dreamhost DreamObjects",
			}, {
				Value: "IBMCOS",
				Help:  "IBM COS S3",
			}, {
				Value: "Minio",
				Help:  "Minio Object Storage",
			}, {
				Value: "Netease",
				Help:  "Netease Object Storage (NOS)",
			}, {
				Value: "RackCorp",
				Help:  "RackCorp Object Storage",
			}, {
				Value: "Scaleway",
				Help:  "Scaleway Object Storage",
			}, {
				Value: "SeaweedFS",
				Help:  "SeaweedFS S3",
			}, {
				Value: "StackPath",
				Help:  "StackPath Object Storage",
			}, {
				Value: "Storj",
				Help:  "Storj (S3 Compatible Gateway)",
			}, {
				Value: "TencentCOS",
				Help:  "Tencent Cloud Object Storage (COS)",
			}, {
				Value: "Wasabi",
				Help:  "Wasabi Object Storage",
			}, {
				Value: "Other",
				Help:  "Any other S3 compatible provider",
			}},
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
			Name: "shared_credentials_file",
			Help: `Path to the shared credentials file.

If env_auth = true then rclone can use a shared credentials file.

If this variable is empty rclone will look for the
"AWS_SHARED_CREDENTIALS_FILE" env variable. If the env value is empty
it will default to the current user's home directory.

    Linux/OSX: "$HOME/.aws/credentials"
    Windows:   "%USERPROFILE%\.aws\credentials"
`,
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
			Name: "v2_auth",
			Help: `If true use v2 authentication.

If this is false (the default) then rclone will use v4 authentication.
If it is set then rclone will use v2 authentication.

Use this only if v4 signatures don't work, e.g. pre Jewel/v10 CEPH.`,
			Default:  false,
			Advanced: true,
		}, {
			Name:     "use_accelerate_endpoint",
			Provider: "AWS",
			Help: `If true use the AWS S3 accelerated endpoint.

See: [AWS S3 Transfer acceleration](https://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration-examples.html)`,
			Default:  false,
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
			Name: "list_url_encode",
			Help: `Whether to url encode listings: true/false/unset

Some providers support URL encoding listings and where this is
available this is more reliable when using control characters in file
names. If this is set to unset (the default) then rclone will choose
according to the provider setting what to apply, but you can override
rclone's choice here.
`,
			Default:  fs.Tristate{},
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

// Constants
const (
	metaMtime   = "Mtime"     // the meta key to store mtime in - e.g. X-Amz-Meta-Mtime
	metaMD5Hash = "Md5chksum" // the meta key to store md5hash in

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
	Provider              string               `config:"provider"`
	Region                string               `config:"region"`
	BucketACL             string               `config:"bucket_acl"`
	UploadCutoff          fs.SizeSuffix        `config:"upload_cutoff"`
	ChunkSize             fs.SizeSuffix        `config:"chunk_size"`
	MaxUploadParts        int64                `config:"max_upload_parts"`
	DisableChecksum       bool                 `config:"disable_checksum"`
	UploadConcurrency     int                  `config:"upload_concurrency"`
	ForcePathStyle        bool                 `config:"force_path_style"`
	AccelerateEndpoint___ bool                 `config:"use_accelerate_endpoint"`
	LeavePartsOnError     bool                 `config:"leave_parts_on_error"`
	ListChunk             int64                `config:"list_chunk"`
	ListVersion           int                  `config:"list_version"`
	ListURLEncode         fs.Tristate          `config:"list_url_encode"`
	NoCheckBucket         bool                 `config:"no_check_bucket"`
	NoHead                bool                 `config:"no_head"`
	NoHeadObject          bool                 `config:"no_head_object"`
	Enc                   encoder.MultiEncoder `config:"encoding"`
	MemoryPoolFlushTime   fs.Duration          `config:"memory_pool_flush_time"`
	MemoryPoolUseMmap     bool                 `config:"memory_pool_use_mmap"`
	DisableHTTP2          bool                 `config:"disable_http2"`
	DownloadURL           string               `config:"download_url"`
}

// Fs represents a remote s3 server
type Fs struct {
	name          string         // the name of the remote
	root          string         // root of the bucket - ignore all objects above this
	opt           Options        // parsed options
	ci            *fs.ConfigInfo // global config
	features      *fs.Features   // optional features
	c             *s3.S3         // the connection to the s3 server
	rootBucket    string         // bucket part of root (if any)
	rootDirectory string         // directory part of root (if any)
	cache         *bucket.Cache  // cache for bucket creation status
	pacer         *fs.Pacer      // To pace the API calls
	srv           *http.Client   // a plain http client
	srvRest       *rest.Client   // the rest connection to the server
	pool          *pool.Pool     // memory pool
	etagIsNotMD5  bool           // if set ETags are not MD5s
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
	meta         map[string]*string // The object metadata if known - may be nil
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
		return fmt.Sprintf("S3 root")
	}
	if f.rootDirectory == "" {
		return fmt.Sprintf("S3 bucket %s", f.rootBucket)
	}
	return fmt.Sprintf("S3 bucket %s path %s", f.rootBucket, f.rootDirectory)
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

// getClient makes an http client according to the options
func getClient(ctx context.Context, opt *Options) *http.Client {
	// TODO: Do we need cookies too?
	t := fshttp.NewTransportCustom(ctx, func(t *http.Transport) {
		if opt.DisableHTTP2 {
			t.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		}
	})
	return &http.Client{
		Transport: t,
	}
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

// Set the provider quirks
//
// There should be no testing against opt.Provider anywhere in the
// code except in here to localise the setting of the quirks.
//
// These should be differences from AWS S3
func setQuirks(opt *Options) {
	var (
		listObjectsV2     = true
		virtualHostStyle  = true
		urlEncodeListings = true
	)
	switch opt.Provider {
	case "AWS":
		// No quirks
	case "Alibaba":
		// No quirks
	case "Ceph":
		listObjectsV2 = false
		virtualHostStyle = false
		urlEncodeListings = false
	case "DigitalOcean":
		urlEncodeListings = false
	case "Dreamhost":
		urlEncodeListings = false
	case "IBMCOS":
		listObjectsV2 = false // untested
		virtualHostStyle = false
		urlEncodeListings = false
	case "Minio":
		virtualHostStyle = false
	case "Netease":
		listObjectsV2 = false // untested
		urlEncodeListings = false
	case "RackCorp":
		// No quirks
	case "Scaleway":
		// Scaleway can only have 1000 parts in an upload
		if opt.MaxUploadParts > 1000 {
			opt.MaxUploadParts = 1000
		}
		urlEncodeListings = false
	case "SeaweedFS":
		listObjectsV2 = false // untested
		virtualHostStyle = false
		urlEncodeListings = false
	case "StackPath":
		listObjectsV2 = false // untested
		virtualHostStyle = false
		urlEncodeListings = false
	case "Storj":
		// Force chunk size to >= 64 MiB
		if opt.ChunkSize < 64*fs.Mebi {
			opt.ChunkSize = 64 * fs.Mebi
		}
	case "TencentCOS":
		listObjectsV2 = false // untested
	case "Wasabi":
		// No quirks
	case "Other":
		listObjectsV2 = false
		virtualHostStyle = false
		urlEncodeListings = false
	default:
		fs.Logf("s3", "s3 provider %q not known - please set correctly", opt.Provider)
		listObjectsV2 = false
		virtualHostStyle = false
		urlEncodeListings = false
	}

	// Path Style vs Virtual Host style
	if virtualHostStyle {
		opt.ForcePathStyle = false
	}

	// Set to see if we need to URL encode listings
	if !opt.ListURLEncode.Valid {
		opt.ListURLEncode.Valid = true
		opt.ListURLEncode.Value = urlEncodeListings
	}

	// Set the correct list version if not manually set
	if opt.ListVersion == 0 {
		if listObjectsV2 {
			opt.ListVersion = 2
		} else {
			opt.ListVersion = 1
		}
	}
}

// setRoot changes the root of the Fs
func (f *Fs) setRoot(root string) {
	f.root = parsePath(root)
	f.rootBucket, f.rootDirectory = bucket.Split(f.root)
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
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
	srv := getClient(ctx, opt)
	ci := fs.GetConfig(ctx)

	lowTimeoutClient := &http.Client{Timeout: 1 * time.Second}
	_ = ci
	_ = lowTimeoutClient
	setQuirks(opt)

	pc := fs.NewPacer(ctx, pacer.NewS3(pacer.MinSleep(minSleep)))
	// Set pacer retries to 2 (1 try and 1 retry) because we are
	// relying on SDK retry mechanism, but we allow 2 attempts to
	// retry directory listings after XMLSyntaxError
	pc.SetRetries(2)

	f := &Fs{
		name:    name,
		opt:     *opt,
		ci:      ci,
		c:       nil,
		pacer:   pc,
		cache:   bucket.NewCache(),
		srv:     srv,
		srvRest: rest.NewClient(fshttp.NewClient(ctx)),
		pool: pool.New(
			time.Duration(opt.MemoryPoolFlushTime),
			int(opt.ChunkSize),
			opt.UploadConcurrency*ci.Transfers,
			opt.MemoryPoolUseMmap,
		),
	}
	f.setRoot(root)
	f.features = (&fs.Features{
		BucketBased:       true,
		BucketBasedRootOK: true,
		SetTier:           false,
		GetTier:           false,
		SlowModTime:       true,
	}).Fill(ctx, f)
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
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *s3.Object) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	if info != nil {
		// Set info but not meta
		if info.LastModified == nil {
			fs.Logf(o, "Failed to read last modified")
			o.lastModified = time.Now()
		} else {
			o.lastModified = *info.LastModified
		}
		o.setMD5FromEtag(aws.StringValue(info.ETag))
		o.bytes = aws.Int64Value(info.Size)
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
type listFn func(remote string, object *s3.Object, isDirectory bool) error

// list lists the objects into the function supplied from
// the bucket and directory supplied.  The remote has prefix
// removed from it and if addBucket is set then it adds the
// bucket to the start.
//
// Set recurse to read sub directories
func (f *Fs) list(ctx context.Context, bucket, directory, prefix string, addBucket bool, recurse bool, fn listFn) error {
	v1 := f.opt.ListVersion == 1
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
	var continuationToken, startAfter *string
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
	urlEncodeListings := f.opt.ListURLEncode.Value
	for {
		// FIXME need to implement ALL loop
		req := s3.ListObjectsV2Input{
			Bucket:            &bucket,
			ContinuationToken: continuationToken,
			Delimiter:         &delimiter,
			Prefix:            &directory,
			MaxKeys:           &f.opt.ListChunk,
			StartAfter:        startAfter,
		}
		if urlEncodeListings {
			req.EncodingType = aws.String(s3.EncodingTypeUrl)
		}
		var resp *s3.ListObjectsV2Output
		var err error
		err = f.pacer.Call(func() (bool, error) {
			if v1 {
				// Convert v2 req into v1 req
				var reqv1 s3.ListObjectsInput
				structs.SetFrom(&reqv1, &req)
				reqv1.Marker = continuationToken
				if startAfter != nil {
					reqv1.Marker = startAfter
				}
				var respv1 *s3.ListObjectsOutput
				respv1, err = f.c.ListObjectsWithContext(ctx, &reqv1)
				if err == nil && respv1 != nil {
					// convert v1 resp into v2 resp
					resp = new(s3.ListObjectsV2Output)
					structs.SetFrom(resp, respv1)
					resp.NextContinuationToken = respv1.NextMarker
				}
			} else {
				resp, err = f.c.ListObjectsV2WithContext(ctx, &req)
			}
			if err != nil && !urlEncodeListings {
				if _, ok := err.(*json.SyntaxError); ok {
					// Retry the listing with URL encoding as there were characters that XML can't encode
					urlEncodeListings = true
					req.EncodingType = aws.String(s3.EncodingTypeUrl)
					fs.Debugf(f, "Retrying listing because of characters which can't be XML encoded")
					return true, err
				}
			}
			return f.shouldRetry(ctx, err)
		})
		if err != nil {
			if awsErr, ok := err.(awserr.RequestFailure); ok {
				if awsErr.StatusCode() == http.StatusNotFound {
					err = fs.ErrorDirNotFound
				}
			}
			if f.rootBucket == "" {
				// if listing from the root ignore wrong region requests returning
				// empty directory
				if reqErr, ok := err.(awserr.RequestFailure); ok {
					// 301 if wrong region for bucket
					if reqErr.StatusCode() == http.StatusMovedPermanently {
						fs.Errorf(f, "Can't change region for bucket %q with no bucket specified", bucket)
						return nil
					}
				}
			}
			return err
		}
		if !recurse {
			for _, commonPrefix := range resp.CommonPrefixes {
				if commonPrefix.Prefix == nil {
					fs.Logf(f, "Nil common prefix received")
					continue
				}
				remote := *commonPrefix.Prefix
				if urlEncodeListings {
					remote, err = url.QueryUnescape(remote)
					if err != nil {
						fs.Logf(f, "failed to URL decode %q in listing common prefix: %v", *commonPrefix.Prefix, err)
						continue
					}
				}
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
				err = fn(remote, &s3.Object{Key: &remote}, true)
				if err != nil {
					return err
				}
			}
		}
		for _, object := range resp.Contents {
			remote := aws.StringValue(object.Key)
			if urlEncodeListings {
				remote, err = url.QueryUnescape(remote)
				if err != nil {
					fs.Logf(f, "failed to URL decode %q in listing: %v", aws.StringValue(object.Key), err)
					continue
				}
			}
			remote = f.opt.Enc.ToStandardPath(remote)
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
			if isDirectory && object.Size != nil && *object.Size == 0 {
				continue // skip directory marker
			}
			err = fn(remote, object, false)
			if err != nil {
				return err
			}
		}
		if !aws.BoolValue(resp.IsTruncated) {
			break
		}
		// Use NextContinuationToken if set, otherwise use last Key for StartAfter
		if resp.NextContinuationToken == nil || *resp.NextContinuationToken == "" {
			if len(resp.Contents) == 0 {
				return errors.New("s3 protocol error: received listing with IsTruncated set, no NextContinuationToken/NextMarker and no Contents")
			}
			continuationToken = nil
			startAfter = resp.Contents[len(resp.Contents)-1].Key
		} else {
			continuationToken = resp.NextContinuationToken
			startAfter = nil
		}
		if startAfter != nil && urlEncodeListings {
			*startAfter, err = url.QueryUnescape(*startAfter)
			if err != nil {
				return fmt.Errorf("failed to URL decode StartAfter/NextMarker %q: %w", *continuationToken, err)
			}
		}
	}
	return nil
}

// Convert a list item into a DirEntry
func (f *Fs) itemToDirEntry(ctx context.Context, remote string, object *s3.Object, isDirectory bool) (fs.DirEntry, error) {
	if isDirectory {
		size := int64(0)
		if object.Size != nil {
			size = *object.Size
		}
		d := fs.NewDir(remote, time.Time{}).SetSize(size)
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
	err = f.list(ctx, bucket, directory, prefix, addBucket, false, func(remote string, object *s3.Object, isDirectory bool) error {
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
	req := s3.ListBucketsInput{}
	var resp *s3.ListBucketsOutput
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.c.ListBucketsWithContext(ctx, &req)
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		return nil, err
	}
	for _, bucket := range resp.Buckets {
		bucketName := f.opt.Enc.ToStandardName(aws.StringValue(bucket.Name))
		f.cache.MarkOK(bucketName)
		d := fs.NewDir(bucketName, aws.TimeValue(bucket.CreationDate))
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
		return f.list(ctx, bucket, directory, prefix, addBucket, true, func(remote string, object *s3.Object, isDirectory bool) error {
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
	req := s3.HeadBucketInput{
		Bucket: &bucket,
	}
	err := f.pacer.Call(func() (bool, error) {
		_, err := f.c.HeadBucketWithContext(ctx, &req)
		return f.shouldRetry(ctx, err)
	})
	if err == nil {
		return true, nil
	}
	if err, ok := err.(awserr.RequestFailure); ok {
		if err.StatusCode() == http.StatusNotFound {
			return false, nil
		}
	}
	return false, err
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
		req := s3.CreateBucketInput{
			Bucket: &bucket,
			ACL:    &f.opt.BucketACL,
		}
		err := f.pacer.Call(func() (bool, error) {
			_, err := f.c.CreateBucketWithContext(ctx, &req)
			return f.shouldRetry(ctx, err)
		})
		if err == nil {
			fs.Infof(f, "Bucket %q created with ACL %q", bucket, f.opt.BucketACL)
		}
		if awsErr, ok := err.(awserr.Error); ok {
			if code := awsErr.Code(); code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists" {
				err = nil
			}
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
	return f.cache.Remove(bucket, func() error {
		req := s3.DeleteBucketInput{
			Bucket: &bucket,
		}
		err := f.pacer.Call(func() (bool, error) {
			_, err := f.c.DeleteBucketWithContext(ctx, &req)
			return f.shouldRetry(ctx, err)
		})
		if err == nil {
			fs.Infof(f, "Bucket %q deleted", bucket)
		}
		return err
	})
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
	httpReq, _ := f.c.GetObjectRequest(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &bucketPath,
	})

	return httpReq.Presign(time.Duration(expire))
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
	if o.fs.etagIsNotMD5 {
		o.md5 = ""
		return
	}
	if etag == "" {
		o.md5 = ""
		return
	}
	hash := strings.Trim(strings.ToLower(etag), `"`)
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

func (o *Object) headObject(ctx context.Context) (resp *s3.HeadObjectOutput, err error) {
	bucket, bucketPath := o.split()
	req := s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &bucketPath,
	}
	err = o.fs.pacer.Call(func() (bool, error) {
		var err error
		resp, err = o.fs.c.HeadObjectWithContext(ctx, &req)
		return o.fs.shouldRetry(ctx, err)
	})
	if err != nil {
		return nil, err
	}
	// TODO
	var hr http.Response
	if hr.StatusCode == http.StatusNotFound {
		return nil, fs.ErrorObjectNotFound
	}
	o.fs.cache.MarkOK(bucket)
	return resp, nil
}

// readMetaData gets the metadata if it hasn't already been fetched
//
// it also sets the info
func (o *Object) readMetaData(ctx context.Context) (err error) {
	if o.meta != nil {
		return nil
	}
	resp, err := o.headObject(ctx)
	if err != nil {
		return err
	}
	o.setMetaData(resp.ETag, resp.ContentLength, resp.LastModified, resp.Metadata, resp.ContentType)
	return nil
}

func (o *Object) setMetaData(etag *string, contentLength *int64, lastModified *time.Time, meta map[string]*string, mimeType *string) {
	// Ignore missing Content-Length assuming it is 0
	// Some versions of ceph do this due their apache proxies
	if contentLength != nil {
		o.bytes = *contentLength
	}
	o.setMD5FromEtag(aws.StringValue(etag))
	o.meta = meta
	if o.meta == nil {
		o.meta = map[string]*string{}
	}
	// Read MD5 from metadata if present
	if md5sumBase64, ok := o.meta[metaMD5Hash]; ok {
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
	d, ok := o.meta[metaMtime]
	if !ok || d == nil {
		// fs.Debugf(o, "No metadata")
		return o.lastModified
	}
	modTime, err := swift.FloatStringToTime(*d)
	if err != nil {
		fs.Logf(o, "Failed to read mtime from object: %v", err)
		return o.lastModified
	}
	return modTime
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

// Storable returns a boolean indicating if this object is storable
func (o *Object) Storable() bool {
	return true
}

func (o *Object) downloadFromURL(ctx context.Context, bucketPath string, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	url := o.fs.opt.DownloadURL + bucketPath
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

	contentType := resp.Header.Get("Content-Type")
	etag := resp.Header.Get("Etag")

	o.setMetaData(&etag, contentLength, &lastModified, metaData, &contentType)
	return resp.Body, err
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	bucket, bucketPath := o.split()

	if o.fs.opt.DownloadURL != "" {
		return o.downloadFromURL(ctx, bucketPath, options...)
	}

	req := s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &bucketPath,
	}
	httpReq, resp := o.fs.c.GetObjectRequest(&req)
	fs.FixRangeOption(options, o.bytes)
	for _, option := range options {
		switch option.(type) {
		case *fs.RangeOption, *fs.SeekOption:
			_, value := option.Header()
			req.Range = &value
		case *fs.HTTPOption:
			key, value := option.Header()
			httpReq.HTTPRequest.Header.Add(key, value)
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}
	err = o.fs.pacer.Call(func() (bool, error) {
		var err error
		httpReq.HTTPRequest = httpReq.HTTPRequest.WithContext(ctx)
		err = httpReq.Send()
		return o.fs.shouldRetry(ctx, err)
	})
	if err != nil {
		return nil, err
	}

	// read size from ContentLength or ContentRange
	size := resp.ContentLength
	if resp.ContentRange != nil {
		var contentRange = *resp.ContentRange
		slash := strings.IndexRune(contentRange, '/')
		if slash >= 0 {
			i, err := strconv.ParseInt(contentRange[slash+1:], 10, 64)
			if err == nil {
				size = &i
			} else {
				fs.Debugf(o, "Failed to find parse integer from in %q: %v", contentRange, err)
			}
		} else {
			fs.Debugf(o, "Failed to find length in %q", contentRange)
		}
	}
	o.setMetaData(resp.ETag, size, resp.LastModified, resp.Metadata, resp.ContentType)
	return resp.Body, nil
}

var warnStreamUpload sync.Once

func (o *Object) uploadMultipart(ctx context.Context, size int64, in io.Reader) (etag string, err error) {
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

	var cout *s3.CreateMultipartUploadOutput
	err = f.pacer.Call(func() (bool, error) {
		var err error
		cout, err = f.c.CreateMultipartUploadWithContext(ctx, nil)
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		return etag, fmt.Errorf("multipart upload failed to initialise: %w", err)
	}
	uid := cout.UploadId

	defer atexit.OnError(&err, func() {
		if o.fs.opt.LeavePartsOnError {
			return
		}
		fs.Debugf(o, "Cancelling multipart upload")
		errCancel := f.pacer.Call(func() (bool, error) {
			_, err := f.c.AbortMultipartUploadWithContext(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:       req.Bucket,
				Key:          req.Key,
				UploadId:     uid,
				RequestPayer: req.RequestPayer,
			})
			return f.shouldRetry(ctx, err)
		})
		if errCancel != nil {
			fs.Debugf(o, "Failed to cancel multipart upload: %v", errCancel)
		}
	})()

	var (
		g, gCtx  = errgroup.WithContext(ctx)
		finished = false
		partsMu  sync.Mutex // to protect parts
		parts    []*s3.CompletedPart
		off      int64
		md5sMu   sync.Mutex
		md5s     []byte
	)

	addMd5 := func(md5binary *[md5.Size]byte, partNum int64) {
		md5sMu.Lock()
		defer md5sMu.Unlock()
		start := partNum * md5.Size
		end := start + md5.Size
		if extend := end - int64(len(md5s)); extend > 0 {
			md5s = append(md5s, make([]byte, extend)...)
		}
		copy(md5s[start:end], (*md5binary)[:])
	}

	for partNum := int64(1); !finished; partNum++ {
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
			return etag, fmt.Errorf("multipart upload failed to read source: %w", err)
		}
		buf = buf[:n]

		partNum := partNum
		fs.Debugf(o, "multipart upload starting chunk %d size %v offset %v/%v", partNum, fs.SizeSuffix(n), fs.SizeSuffix(off), fs.SizeSuffix(size))
		off += int64(n)
		g.Go(func() (err error) {
			defer free()
			partLength := int64(len(buf))

			// create checksum of buffer for integrity checking
			md5sumBinary := md5.Sum(buf)
			addMd5(&md5sumBinary, partNum-1)
			md5sum := base64.StdEncoding.EncodeToString(md5sumBinary[:])

			err = f.pacer.Call(func() (bool, error) {
				uploadPartReq := &s3.UploadPartInput{
					Body:                 bytes.NewReader(buf),
					Bucket:               req.Bucket,
					Key:                  req.Key,
					PartNumber:           &partNum,
					UploadId:             uid,
					ContentMD5:           &md5sum,
					ContentLength:        &partLength,
					RequestPayer:         req.RequestPayer,
					SSECustomerAlgorithm: req.SSECustomerAlgorithm,
					SSECustomerKey:       req.SSECustomerKey,
					SSECustomerKeyMD5:    req.SSECustomerKeyMD5,
				}
				uout, err := f.c.UploadPartWithContext(gCtx, uploadPartReq)
				if err != nil {
					if partNum <= int64(concurrency) {
						return f.shouldRetry(ctx, err)
					}
					// retry all chunks once have done the first batch
					return true, err
				}
				partsMu.Lock()
				parts = append(parts, &s3.CompletedPart{
					PartNumber: &partNum,
					ETag:       uout.ETag,
				})
				partsMu.Unlock()

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
		return etag, err
	}

	// sort the completed parts by part number
	sort.Slice(parts, func(i, j int) bool {
		return *parts[i].PartNumber < *parts[j].PartNumber
	})

	err = f.pacer.Call(func() (bool, error) {
		_, err := f.c.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: req.Bucket,
			Key:    req.Key,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: parts,
			},
			RequestPayer: req.RequestPayer,
			UploadId:     uid,
		})
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		return etag, fmt.Errorf("multipart upload failed to finalise: %w", err)
	}
	hashOfHashes := md5.Sum(md5s)
	etag = fmt.Sprintf("%s-%d", hex.EncodeToString(hashOfHashes[:]), len(parts))
	return etag, nil
}

// Update the Object from in with modTime and size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	bucket, bucketPath := o.split()
	err := o.fs.makeBucket(ctx, bucket)
	if err != nil {
		return err
	}
	modTime := src.ModTime(ctx)
	size := src.Size()

	multipart := size < 0 || size >= int64(o.fs.opt.UploadCutoff)

	// Set the mtime in the meta data
	metadata := map[string]*string{
		metaMtime: aws.String(swift.TimeToFloatString(modTime)),
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
				if (multipart || o.fs.etagIsNotMD5) && !o.fs.opt.DisableChecksum {
					// Set the md5sum as metadata on the object if
					// - a multipart upload
					// - the Etag is not an MD5, eg when using SSE/SSE-C
					// provided checksums aren't disabled
					metadata[metaMD5Hash] = &md5sumBase64
				}
			}
		}
	}

	// Apply upload options
	for _, option := range options {
		// TODO
		_, _ = option.Header()
	}

	var resp *http.Response // response from PUT
	var wantETag string     // Multipart upload Etag to check
	if multipart {
		wantETag, err = o.uploadMultipart(ctx, size, in)
		if err != nil {
			return err
		}
	} else {

		// Create the request
		putObj, _ := o.fs.c.PutObjectRequest(&req)

		// Sign it so we can upload using a presigned request.
		//
		// Note the SDK doesn't currently support streaming to
		// PutObject so we'll use this work-around.
		url, headers, err := putObj.PresignRequest(15 * time.Minute)
		if err != nil {
			return fmt.Errorf("s3 upload: sign request: %w", err)
		}

		if headers == nil {
			headers = putObj.HTTPRequest.Header
		}

		// Set request to nil if empty so as not to make chunked encoding
		if size == 0 {
			in = nil
		}

		// create the vanilla http request
		httpReq, err := http.NewRequestWithContext(ctx, "PUT", url, in)
		if err != nil {
			return fmt.Errorf("s3 upload: new request: %w", err)
		}

		// set the headers we signed and the length
		httpReq.Header = headers
		httpReq.ContentLength = size

		err = o.fs.pacer.CallNoRetry(func() (bool, error) {
			var err error
			resp, err = o.fs.srv.Do(httpReq)
			if err != nil {
				return o.fs.shouldRetry(ctx, err)
			}
			body, err := rest.ReadBody(resp)
			if err != nil {
				return o.fs.shouldRetry(ctx, err)
			}
			if resp.StatusCode >= 200 && resp.StatusCode < 299 {
				return false, nil
			}
			err = fmt.Errorf("s3 upload: %s: %s", resp.Status, body)
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
		o.meta = req.Metadata
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
	o.meta = nil // wipe old metadata
	head, err := o.headObject(ctx)
	if err != nil {
		return err
	}
	o.setMetaData(head.ETag, head.ContentLength, head.LastModified, head.Metadata, head.ContentType)
	if !o.fs.etagIsNotMD5 && wantETag != "" && head.ETag != nil && *head.ETag != "" {
		gotETag := strings.Trim(strings.ToLower(*head.ETag), `"`)
		if wantETag != gotETag {
			return fmt.Errorf("multipart upload corrupted: Etag differ: expecting %s but got %s", wantETag, gotETag)
		}
		fs.Debugf(o, "Multipart upload Etag: %s OK", wantETag)
	}
	return err
}

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	opts := rest.Opts{
		Method: http.MethodDelete,
		Path:   o.remote,
	}
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srvRest.Call(ctx, &opts)
		return o.fs.shouldRetry(ctx, err)
	})
	return err
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
