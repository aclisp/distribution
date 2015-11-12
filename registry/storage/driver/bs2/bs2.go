package bs2

import (
	"errors"
	"fmt"
	"github.com/aclisp/go-bs2"
	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

const driverName = "bs2"

// minChunkSize defines the minimum multipart upload chunk size
// BS2 API requires multipart upload chunks to be at least 100KB
const minChunkSize = 100 * 1024

const defaultChunkSize = 2 * minChunkSize

// smallFileMaxSize defines the maximum size of a BS2 "small" file.
// Files that smaller than this value are BS2 "small" files.
// Files that equal to or larger than this value are BS2 "large" files.
const smallFileMaxSize = 16 * 1024 * 1024

func init() {
	factory.Register(driverName, &bs2DriverFactory{})
}

// bs2DriverFactory implements the factory.StorageDriverFactory interface
type bs2DriverFactory struct{}

func (factory *bs2DriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

//DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	AccessKey   string
	SecretKey   string
	SmallBucket string
	LargeBucket string
	ChunkSize   int64
}

type driver struct {
	Conn        bs2.Connection
	SmallBucket string
	LargeBucket string
	ChunkSize   int64

	pool  sync.Pool // pool []byte buffers used for WriteStream
	zeros []byte    // shared, zero-valued buffer used for WriteStream

	pathSet map[string]bool // remember all the paths, because BS2 does not have "LIST" API
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by YY BS2
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - accesskey
// - secretkey
// - smallBucket
// - largeBucket
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	accessKey, ok := parameters["accesskey"]
	if !ok || fmt.Sprint(accessKey) == "" {
		return nil, fmt.Errorf("No access key provided")
	}

	secretKey, ok := parameters["secretkey"]
	if !ok || fmt.Sprint(secretKey) == "" {
		return nil, fmt.Errorf("No secret key provided")
	}

	smallBucket, ok := parameters["smallbucket"]
	if !ok || fmt.Sprint(smallBucket) == "" {
		return nil, fmt.Errorf("No small-file bucket parameter provided")
	}

	largeBucket, ok := parameters["largebucket"]
	if !ok || fmt.Sprint(largeBucket) == "" {
		return nil, fmt.Errorf("No large-file bucket parameter provided")
	}

	chunkSize := int64(defaultChunkSize)
	chunkSizeParam, ok := parameters["chunksize"]
	if ok {
		switch v := chunkSizeParam.(type) {
		case string:
			vv, err := strconv.ParseInt(v, 0, 64)
			if err != nil {
				return nil, fmt.Errorf("chunksize parameter must be an integer, %v invalid", chunkSizeParam)
			}
			chunkSize = vv
		case int64:
			chunkSize = v
		case int, uint, int32, uint32, uint64:
			chunkSize = reflect.ValueOf(v).Convert(reflect.TypeOf(chunkSize)).Int()
		default:
			return nil, fmt.Errorf("invalid valud for chunksize: %#v", chunkSizeParam)
		}

		if chunkSize < minChunkSize {
			return nil, fmt.Errorf("The chunksize %#v parameter should be a number that is larger than or equal to %d", chunkSize, minChunkSize)
		}
	}

	params := DriverParameters{
		fmt.Sprint(accessKey),
		fmt.Sprint(secretKey),
		fmt.Sprint(smallBucket),
		fmt.Sprint(largeBucket),
		chunkSize,
	}

	return New(params), nil
}

// New constructs a new Driver with the given BS2 credentials and bucket names
func New(params DriverParameters) *Driver {
	d := &driver{
		Conn: bs2.Connection{
			AccessKey: params.AccessKey,
			SecretKey: params.SecretKey,
		},
		SmallBucket: params.SmallBucket,
		LargeBucket: params.LargeBucket,
		ChunkSize:   params.ChunkSize,
		zeros:       make([]byte, params.ChunkSize),
		pathSet:     make(map[string]bool),
	}

	d.pool.New = func() interface{} {
		return make([]byte, d.ChunkSize)
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}
}

// Implement the storagedriver.StorageDriver interface

// Name returns the human-readable "name" of the driver, useful in error
// messages and logging. By convention, this will just be the registration
// name, but drivers may provide other information here.
func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
// This should primarily be used for small objects.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	path = d.bs2Path(path, bs2PathGet)
	return d.Conn.ObjectGetBytes(d.SmallBucket, path)
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	if len(contents) >= smallFileMaxSize {
		return errors.New("PutContent was called with too large file")
	}
	path = d.bs2Path(path, bs2PathPut)
	return d.Conn.ObjectPutBytes(d.SmallBucket, path, contents, d.getContentType())
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	return nil, nil
}

// WriteStream stores the contents of the provided io.ReadCloser at a
// location designated by the given path.
// May be used to resume writing a stream by providing a nonzero offset.
// The offset must be no larger than the CurrentSize for this path.
func (d *driver) WriteStream(ctx context.Context, path string, offset int64, reader io.Reader) (totalRead int64, err error) {
	return 0, nil
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	return nil, nil
}

// List returns a list of the objects that are direct descendants of the
//given path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	return nil, nil
}

// Move moves an object stored at sourcePath to destPath, removing the
// original object.
// Note: This may be no more efficient than a copy followed by a delete for
// many implementations.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	return nil
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at
// the given path, possibly using the given options.
// May return an ErrUnsupportedMethod in certain StorageDriver
// implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "", nil
}

func (d *driver) getContentType() string {
	return "application/octet-stream"
}

// getbuf returns a buffer from the driver's pool with length d.ChunkSize.
func (d *driver) getbuf() []byte {
	return d.pool.Get().([]byte)
}

func (d *driver) putbuf(p []byte) {
	copy(p, d.zeros)
	d.pool.Put(p)
}

type bs2PathAction int

const (
	bs2PathGet = iota
	bs2PathPut = iota
	bs2PathDel = iota
)

func (d *driver) bs2Path(path string, action bs2PathAction) string {
	// Remember the path locally
	switch action {
	case bs2PathPut:
		d.pathSet[path] = true
	case bs2PathDel:
		delete(d.pathSet, path)
	}
	// Return the path with leading slash trimmed, which is required by BS2 API.
	return strings.TrimLeft(path, "/")
}
