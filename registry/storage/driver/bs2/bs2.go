package bs2

import (
	"bytes"
	"fmt"
	"github.com/aclisp/go-bs2"
	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const driverName = "bs2"

// minChunkSize defines the minimum multipart upload chunk size
// BS2 API requires multipart upload chunks to be at least 100KB
const minChunkSize = 100 * 1024

//const minChunkSize = 6

const defaultChunkSize = 2 * minChunkSize

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
	AccessKey string
	SecretKey string
	Bucket    string
	ChunkSize int64
}

type driver struct {
	Conn      bs2.Connection
	Bucket    string
	ChunkSize int64

	pool     sync.Pool       // pool []byte buffers used for WriteStream
	zeros    []byte          // shared, zero-valued buffer used for WriteStream
	pathSet  map[string]bool // remember all the paths, because BS2 does not have "LIST" API
	pathLock sync.Mutex
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

	bucket, ok := parameters["bucket"]
	if !ok || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
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
		fmt.Sprint(bucket),
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
			Logger:    log.New(os.Stdout, "", 0),
		},
		Bucket:    params.Bucket,
		ChunkSize: params.ChunkSize,
		zeros:     make([]byte, params.ChunkSize),
		pathSet:   make(map[string]bool),
	}

	d.pool.New = func() interface{} {
		return make([]byte, d.ChunkSize)
	}

	d.loadPathSet()

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
func (d *driver) GetContent(ctx context.Context, path string) (contents []byte, err error) {
	d.Conn.Logger.SetPrefix("GC: ")
	defer func() {
		d.Conn.Logger.SetPrefix("")
	}()

	d.pathLock.Lock()
	if !d.pathSet[path] {
		d.pathLock.Unlock()
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	d.pathLock.Unlock()

	for i := 0; i < 5; i++ {
		var buf bytes.Buffer
		_, err = d.Conn.ObjectGet(d.Bucket, d.bs2Path(path), &buf, false, nil)
		contents = buf.Bytes()
		if err == bs2.ObjectNotFound {
			time.Sleep(1 * time.Second)
			continue
		}
		return
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) (err error) {
	d.Conn.Logger.SetPrefix("PC: ")
	defer func() {
		d.Conn.Logger.SetPrefix("")
	}()

	err = d.Conn.ObjectPutBytes(d.Bucket, d.bs2Path(path), contents, d.getContentType())
	if err != nil {
		return
	}
	d.pathLock.Lock()
	d.pathSet[path] = true
	d.pathLock.Unlock()

	d.savePathSet()
	time.Sleep(5 * time.Second)
	return
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (rc io.ReadCloser, err error) {
	d.Conn.Logger.SetPrefix("RS: ")
	defer func() {
		d.Conn.Logger.SetPrefix("")
	}()

	d.pathLock.Lock()
	if !d.pathSet[path] {
		d.pathLock.Unlock()
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	d.pathLock.Unlock()

	headers := make(bs2.Headers)
	headers["Range"] = "bytes=" + strconv.FormatInt(offset, 10) + "-"

	for i := 0; i < 5; i++ {
		rc, _, err = d.Conn.ObjectOpen(d.Bucket, d.bs2Path(path), false, headers)
		if err == bs2.ObjectNotFound {
			time.Sleep(1 * time.Second)
			continue
		}
		if bs2Err, ok := err.(*bs2.Error); ok && bs2Err.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			return ioutil.NopCloser(bytes.NewReader(nil)), nil
		}
		return
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// WriteStream stores the contents of the provided io.Reader at a
// location designated by the given path. The driver will know it has
// received the full contents when the reader returns io.EOF. The number
// of successfully READ bytes will be returned, even if an error is
// returned. May be used to resume writing a stream by providing a nonzero
// offset.
// The offset must be no larger than the CurrentSize for this path.
// Offsets past the current size will write from the position
// beyond the end of the file.
func (d *driver) WriteStream(ctx context.Context, path string, offset int64, reader io.Reader) (totalRead int64, err error) {
	d.Conn.Logger.SetPrefix("WS: ")
	defer func() {
		d.Conn.Logger.SetPrefix("")
	}()

	var currentSize int64 = 0
	var partNumber int = -1
	var multi *bs2.Multi

	if offset == 0 {
		multi, err = d.Conn.InitMulti(d.Bucket, d.bs2Path(path), false, d.getContentType())
		if err != nil {
			return 0, err
		}
	} else {
		// Need retry to ensure the last upload parts appear!
		for i := 0; i < 5; i++ {
			multi, err = d.Conn.Multi(d.Bucket, d.bs2Path(path))
			if err == bs2.ObjectNotFound {
				time.Sleep(1 * time.Second)
				continue
			}
			if err != nil {
				return 0, err
			}
			break
		}
		if err != nil {
			return 0, storagedriver.PathNotFoundError{Path: path}
		}
		// Got a multi-part, let's check it status, and resume current size and part number if necessary
		if !multi.Uploading {
			return 0, fmt.Errorf("WriteStream was called to resume uploading at offset (%d), but path (%s) is NOT in uploading state", offset, path)
		}
		lastPart, err := multi.LastPart()
		if err != nil {
			return 0, err
		}
		currentSize, err = strconv.ParseInt(lastPart.CurrentSize, 10, 64)
		if err != nil {
			return 0, err
		}
		partNumber, err = strconv.Atoi(lastPart.PartNumber)
		if err != nil {
			return 0, err
		}
	}

	//d.Conn.Logger.Printf("currentSize=%d, partNumber=%d, offset=%d\n", currentSize, partNumber, offset)
	bufBytes := d.getbuf()
	defer func() {
		d.putbuf(bufBytes)
	}()
	buf := bytes.NewBuffer(bufBytes)

	if offset < currentSize {
		// Drop the data, which has been uploaded, from offset to current size
		n, err := io.Copy(nopWriter{}, io.LimitReader(reader, currentSize-offset))
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		if n == 0 {
			goto done
		}
	}

	if offset > currentSize {
		// We fill the gap, which is from current size to offset , with all zeros!
		gapChunks := (offset - currentSize) / d.ChunkSize
		for i := int64(0); i < gapChunks; i++ {
			partNumber++
			_, err := multi.PutSizedPart(partNumber, bytes.NewReader(d.zeros), d.ChunkSize)
			if err != nil {
				return totalRead, err
			}
		}

		gapRemain := (offset - currentSize) % d.ChunkSize
		buf.Reset()
		io.Copy(buf, io.LimitReader(bytes.NewReader(d.zeros), gapRemain))
		n, err := io.Copy(buf, io.LimitReader(reader, d.ChunkSize-gapRemain))
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		if buf.Len() == 0 {
			goto done
		}

		partNumber++
		part, err := multi.PutSizedPart(partNumber, buf, int64(buf.Len()))
		if err != nil {
			return totalRead, err
		}

		if part.Size != d.ChunkSize {
			goto done
		}
	}

	for {
		buf.Reset()
		n, err := io.Copy(buf, io.LimitReader(reader, d.ChunkSize))
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		if n == 0 {
			goto done
		}

		partNumber++
		part, err := multi.PutSizedPart(partNumber, buf, int64(buf.Len()))
		if err != nil {
			return totalRead, err
		}

		if part.Size != d.ChunkSize {
			goto done
		}
	}

done:
	//partNumber++
	//err := multi.Complete(partNumber)
	d.pathLock.Lock()
	d.pathSet[path] = true
	d.pathLock.Unlock()

	d.savePathSet()
	time.Sleep(5 * time.Second)
	return totalRead, err
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (info storagedriver.FileInfo, err error) {
	fi := storagedriver.FileInfoFields{
		Path: path,
	}
	d.pathLock.Lock()
	if !d.pathSet[path] {
		for file := range d.pathSet {
			if strings.HasPrefix(file, path) {
				fi.IsDir = true
				d.pathLock.Unlock()
				return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
			}
		}
		d.pathLock.Unlock()
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	d.pathLock.Unlock()

	for i := 0; i < 5; i++ {
		var obj bs2.Object
		obj, _, err = d.Conn.Object(d.Bucket, d.bs2Path(path))
		if err == bs2.ObjectNotFound {
			time.Sleep(1 * time.Second)
			continue
		}
		if err != nil {
			return nil, err
		}
		d.pathLock.Lock()
		d.pathSet[path] = true
		d.pathLock.Unlock()
		fi.Size = obj.Bytes
		fi.ModTime = obj.LastModified
		fi.IsDir = false
		return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// List returns a list of the objects that are direct descendants of the
//given path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	if path[len(path)-1] != '/' {
		path = path + "/"
	}

	sub := make(map[string]bool)
	d.pathLock.Lock()
	for file := range d.pathSet {
		if strings.HasPrefix(file, path) {
			file = file[len(path):]
			slash := strings.IndexByte(file, '/')
			if slash != -1 {
				file = file[:slash]
			}
			sub[file] = true
		}
	}
	d.pathLock.Unlock()

	var r []string
	for key := range sub {
		r = append(r, path+key)
	}
	return r, nil
}

// Move moves an object stored at sourcePath to destPath, removing the
// original object.
// Note: This may be no more efficient than a copy followed by a delete for
// many implementations.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) (err error) {
	d.Conn.Logger.SetPrefix("MV: ")
	defer func() {
		d.Conn.Logger.SetPrefix("")
	}()

	d.pathLock.Lock()
	if !d.pathSet[sourcePath] {
		d.pathLock.Unlock()
		return storagedriver.PathNotFoundError{Path: sourcePath}
	}
	d.pathLock.Unlock()

	// {{ Ensure multi-part uploads complete!
	var multi *bs2.Multi
	for i := 0; i < 5; i++ {
		multi, err = d.Conn.Multi(d.Bucket, d.bs2Path(sourcePath))
		if err == bs2.ObjectNotFound {
			time.Sleep(1 * time.Second)
			continue
		}
		if err != nil {
			return err
		}
		break
	}
	if err != nil {
		return storagedriver.PathNotFoundError{Path: sourcePath}
	}
	if multi.Uploading {
		lastPart, err := multi.LastPart()
		if err != nil {
			return err
		}
		partNumber, err := strconv.Atoi(lastPart.PartNumber)
		if err != nil {
			return err
		}
		partNumber++
		err = multi.Complete(partNumber)
		if err != nil {
			return err
		}
	}
	// }}

	for i := 0; i < 5; i++ {
		_, err = d.Conn.ObjectMove(d.Bucket, d.bs2Path(sourcePath), d.Bucket, d.bs2Path(destPath), nil)
		if err == bs2.ObjectNotFound {
			time.Sleep(1 * time.Second)
			continue
		}
		if err != nil {
			return
		}
		d.pathLock.Lock()
		d.pathSet[destPath] = true
		delete(d.pathSet, sourcePath)
		d.pathLock.Unlock()

		d.savePathSet()
		time.Sleep(5 * time.Second)
		return
	}
	return storagedriver.PathNotFoundError{Path: sourcePath}
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) (err error) {
	d.Conn.Logger.SetPrefix("DE: ")
	defer func() {
		d.Conn.Logger.SetPrefix("")
	}()

	var sub []string
	d.pathLock.Lock()
	if !d.pathSet[path] {
		for file := range d.pathSet {
			if strings.HasPrefix(file, path) {
				sub = append(sub, file)
			}
		}
		if len(sub) == 0 {
			d.pathLock.Unlock()
			return storagedriver.PathNotFoundError{Path: path}
		}
	} else {
		sub = append(sub, path)
	}
	d.pathLock.Unlock()

	for _, item := range sub {
		if err = d.Conn.ObjectDelete(d.Bucket, d.bs2Path(item)); err != nil {
			continue
		}
		d.pathLock.Lock()
		delete(d.pathSet, item)
		d.pathLock.Unlock()
	}
	d.savePathSet()
	return
}

// URLFor returns a URL which may be used to retrieve the content stored at
// the given path, possibly using the given options.
// May return an ErrUnsupportedMethod in certain StorageDriver
// implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (URL string, err error) {
	d.pathLock.Lock()
	if !d.pathSet[path] {
		d.pathLock.Unlock()
		return "", storagedriver.PathNotFoundError{Path: path}
	}
	d.pathLock.Unlock()

	methodString := "GET"
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok || (methodString != "GET" && methodString != "HEAD") {
			return "", storagedriver.ErrUnsupportedMethod{}
		}
	}

	expiresTime := time.Now().Add(15 * time.Minute)
	expires, ok := options["expiry"]
	if ok {
		et, ok := expires.(time.Time)
		if ok {
			expiresTime = et
		}
	}

	for i := 0; i < 5; i++ {
		var urls []string
		urls, err = d.Conn.ObjectTempUrl(d.Bucket, d.bs2Path(path), methodString, int64(expiresTime.Sub(time.Now())/time.Second))
		if err == bs2.ObjectNotFound {
			time.Sleep(1 * time.Second)
			continue
		}
		if err != nil {
			return "", err
		}
		return urls[0], nil
	}
	return "", storagedriver.PathNotFoundError{Path: path}
}

func (d *driver) getContentType() string {
	return "application/octet-stream"
}

func (d *driver) bs2Path(path string) string {
	// Return the path with leading slash trimmed, which is required by BS2 API.
	return strings.TrimLeft(path, "/")
}

type nopWriter struct {
}

func (nopWriter) Write(p []byte) (n int, err error) {
	// Do nothing
	return len(p), nil
}

// getbuf returns a buffer from the driver's pool with length d.ChunkSize.
func (d *driver) getbuf() []byte {
	return d.pool.Get().([]byte)
}

func (d *driver) putbuf(p []byte) {
	copy(p, d.zeros)
	d.pool.Put(p)
}

func (d *driver) savePathSet() {
	paths := new(bytes.Buffer)
	d.pathLock.Lock()
	for file := range d.pathSet {
		fmt.Fprintln(paths, file)
	}
	d.pathLock.Unlock()
	contents := paths.Bytes()
	if len(contents) == 0 {
		d.Conn.ObjectPutString(d.Bucket, "__ALL_FILES__", "<nil>", "text/plain")
	} else {
		d.Conn.ObjectPutBytes(d.Bucket, "__ALL_FILES__", contents, "text/plain")
	}
}

func (d *driver) loadPathSet() {
	paths, err := d.Conn.ObjectGetBytes(d.Bucket, "__ALL_FILES__")
	if err != nil {
		return
	}
	if len(paths) == len("<nil>") && string(paths) == "<nil>" {
		return
	}
	reader := bytes.NewReader(paths)
	d.pathLock.Lock()
	for k := range d.pathSet {
		delete(d.pathSet, k)
	}
	for {
		var file string
		_, err := fmt.Fscanln(reader, &file)
		if err != nil {
			break
		}
		d.pathSet[file] = true
	}
	d.pathLock.Unlock()
}
