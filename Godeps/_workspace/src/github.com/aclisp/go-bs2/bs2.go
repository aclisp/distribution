package bs2

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultUserAgent = "go-bs2/1.0"
	DefaultRetries   = 3
	DefaultExpires   = 300

	Host         = "bs2.yy.com"
	HostUpload   = "bs2ul.yy.com"
	HostDownload = "bs2dl.yy.com"
)

// Connection holds the details of the connection to the BS2 server.
//
type Connection struct {
	AccessKey      string
	SecretKey      string
	Retries        int           // Retries on error (default is 3)
	UserAgent      string        // Http User agent (default go-bs2/1.0)
	ConnectTimeout time.Duration // Connect channel timeout (default 10s)
	Timeout        time.Duration // Data channel timeout (default 60s)
	Expires        int64
	Transport      http.RoundTripper
	Logger         *log.Logger

	// These are filled in later
	client *http.Client
}

// Set defaults for any unset values
//
func (c *Connection) setDefaults() {
	if c.UserAgent == "" {
		c.UserAgent = DefaultUserAgent
	}
	if c.Retries == 0 {
		c.Retries = DefaultRetries
	}
	if c.Expires == 0 {
		c.Expires = DefaultExpires
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 10 * time.Second
	}
	if c.Timeout == 0 {
		c.Timeout = 60 * time.Second
	}
	if c.Transport == nil {
		c.Transport = http.DefaultTransport
	}
	if c.client == nil {
		c.client = &http.Client{
			Transport: c.Transport,
		}
	}
}

// Headers stores HTTP headers (can only have one of each header).
type Headers map[string]string

// errorMap defines http error codes to error mappings.
type errorMap map[int]error

// RequestOpts contains parameters for Connection.Call.
type RequestOpts struct {
	BucketName string
	ObjectName string
	Operation  string
	Parameters url.Values
	Headers    Headers
	ErrorMap   errorMap
	NoResponse bool
	Body       io.Reader
	Retries    int
	Expires    int64
}

// Call runs a remote command on the targetHost, returns a
// response, headers and possible error.
//
// operation is GET, HEAD etc
// BucketName is the name of a bucket
// Any other parameters (if not None) are added to the targetHost
//
// Returns a response or an error.  If response is returned then
// resp.Body.Close() must be called on it, unless noResponse is set in
// which case the body will be closed in this function
//
// If "Content-Length" is set in p.Headers it will be used - this can
// be used to override the default chunked transfer encoding for
// uploads.
//
// This method is exported so extensions can call it.
func (c *Connection) Call(targetHost string, p RequestOpts) (resp *http.Response, headers Headers, err error) {
	c.setDefaults()
	retries := p.Retries
	if retries == 0 {
		retries = c.Retries
	}
	expires := p.Expires
	if expires == 0 {
		expires = c.Expires
	}
	var req *http.Request
	for {
		var authToken string
		if authToken, err = c.GetAuthToken(p.Operation, p.BucketName, p.ObjectName, expires); err != nil {
			return
		}
		rawUrl := targetHost
		host := targetHost
		if p.BucketName != "" {
			rawUrl = p.BucketName + "." + rawUrl
			host = rawUrl
			if p.ObjectName != "" {
				rawUrl += "/" + p.ObjectName
			}
		}
		var URL *url.URL
		URL, err = url.Parse(rawUrl)
		if err != nil {
			return
		}
		if URL.Scheme == "" {
			URL.Scheme = "http"
		}
		if p.Parameters != nil {
			URL.RawQuery = encodeUrlParameters(p.Parameters)
		}
		timer := time.NewTimer(c.ConnectTimeout)
		reader := p.Body
		if reader != nil {
			reader = newWatchdogReader(reader, c.Timeout, timer)
		}
		req, err = http.NewRequest(p.Operation, URL.String(), reader)
		if err != nil {
			return
		}
		if p.Headers != nil {
			for k, v := range p.Headers {
				// Set ContentLength in req if the user passed it in in the headers
				if k == "Content-Length" {
					contentLength, err := strconv.ParseInt(v, 10, 64)
					if err != nil {
						return nil, nil, fmt.Errorf("Invalid %q header %q: %v", k, v, err)
					}
					req.ContentLength = contentLength
				}
				req.Header.Add(k, v)
			}
		}
		req.Header.Add("User-Agent", c.UserAgent)
		req.Header.Add("Host", host)
		req.Header.Add("Date", time.Now().UTC().Format(http.TimeFormat))
		req.Header.Add("Authorization", authToken)
		if req.Header.Get("Content-Type") == "" {
			req.Header.Add("Content-Type", "application/octet-stream")
		}
		if c.Logger != nil {
			c.logRequest(req, retries)
		}
		resp, err = c.doTimeoutRequest(timer, req)
		if err != nil {
			if (p.Operation == "HEAD" || p.Operation == "GET") && retries > 0 {
				retries--
				continue
			}
			return nil, nil, err
		}
		if c.Logger != nil {
			c.logResponse(resp, retries)
		}
		// Check to see if token has expired
		if resp.StatusCode == 401 && retries > 0 {
			_ = resp.Body.Close()
			retries--
		} else {
			break
		}
	}
	if err = c.parseHeaders(resp, p.ErrorMap); err != nil {
		_ = resp.Body.Close()
		return nil, nil, err
	}
	headers = readHeaders(resp)
	if p.NoResponse {
		err = resp.Body.Close()
		if err != nil {
			return nil, nil, err
		}
	} else {
		// Cancel the request on timeout
		cancel := func() {
			cancelRequest(c.Transport, req)
		}
		// Wrap resp.Body to make it obey an idle timeout
		resp.Body = newTimeoutReader(resp.Body, c.Timeout, cancel)
	}
	return
}

func (c *Connection) GetAuthToken(operation string, bucketName string, objectName string, expires int64) (authToken string, err error) {
	absoluteExpires := time.Now().Unix() + expires
	combined := fmt.Sprintf("%s\n%s\n%s\n%d\n", operation, bucketName, objectName, absoluteExpires)
	mac := hmac.New(sha1.New, []byte(c.SecretKey))
	_, err = mac.Write([]byte(combined))
	if err != nil {
		return
	}
	authToken = fmt.Sprintf("%s:%s:%d", c.AccessKey, base64.URLEncoding.EncodeToString(mac.Sum(nil)), absoluteExpires)
	return
}

// Encode encodes the values into ``URL encoded'' form
// ("bar=baz&foo=quux") sorted by key.
// This function supports nil key values!
func encodeUrlParameters(v url.Values) string {
	if v == nil {
		return ""
	}
	var buf bytes.Buffer
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vs := v[k]
		if len(vs) == 0 {
			prefix := url.QueryEscape(k)
			if buf.Len() > 0 {
				buf.WriteByte('&')
			}
			buf.WriteString(prefix)
		} else {
			prefix := url.QueryEscape(k) + "="
			for _, v := range vs {
				if buf.Len() > 0 {
					buf.WriteByte('&')
				}
				buf.WriteString(prefix)
				buf.WriteString(url.QueryEscape(v))
			}
		}
	}
	return buf.String()
}

// Does an http request using the running timer passed in
func (c *Connection) doTimeoutRequest(timer *time.Timer, req *http.Request) (*http.Response, error) {
	// Do the request in the background so we can check the timeout
	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := c.client.Do(req)
		done <- result{resp, err}
	}()
	// Wait for the read or the timeout
	select {
	case r := <-done:
		return r.resp, r.err
	case <-timer.C:
		// Kill the connection on timeout so we don't leak sockets or goroutines
		cancelRequest(c.Transport, req)
		return nil, TimeoutError
	}
}

// Cancel the request
func cancelRequest(transport http.RoundTripper, req *http.Request) {
	if tr, ok := transport.(interface {
		CancelRequest(*http.Request)
	}); ok {
		tr.CancelRequest(req)
	}
}

// parseHeaders checks a response for errors and translates into
// standard errors if necessary.
func (c *Connection) parseHeaders(resp *http.Response, errorMap errorMap) error {
	if errorMap != nil {
		if err, ok := errorMap[resp.StatusCode]; ok {
			return err
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return newErrorf(resp.StatusCode, "HTTP Error: %d: %s", resp.StatusCode, resp.Status)
	}
	return nil
}

// readHeaders returns a Headers object from the http.Response.
//
// If it receives multiple values for a key (which should never
// happen) it will use the first one
func readHeaders(resp *http.Response) Headers {
	headers := Headers{}
	for key, values := range resp.Header {
		headers[key] = values[0]
	}
	return headers
}

func (c *Connection) logRequest(req *http.Request, currentRetry int) {
	c.Logger.Printf("%d > %s %s %s", currentRetry, req.Method, req.URL.String(), req.Proto)
	//for key, values := range req.Header {
	//	c.Logger.Printf("%d >   %s: %s", currentRetry, key, strings.Join(values, " && "))
	//}
}

func (c *Connection) logResponse(resp *http.Response, currentRetry int) {
	c.Logger.Printf("%d<  %s %s", currentRetry, resp.Status, resp.Proto)
	//for key, values := range resp.Header {
	//	c.Logger.Printf("%d<    %s: %s", currentRetry, key, strings.Join(values, " && "))
	//}
}

// ------------------------------------------------------------

// ObjectCreateFile represents a BS2 object open for writing
type ObjectCreateFile struct {
	checkHash  bool           // whether we are checking the hash
	pipeReader *io.PipeReader // pipe for the caller to use
	pipeWriter *io.PipeWriter
	hash       hash.Hash      // hash being build up as we go along
	done       chan struct{}  // signals when the upload has finished
	resp       *http.Response // valid when done has signalled
	err        error          // ditto
	headers    Headers        // ditto
}

// Write bytes to the object - see io.Writer
func (file *ObjectCreateFile) Write(p []byte) (n int, err error) {
	n, err = file.pipeWriter.Write(p)
	if err == io.ErrClosedPipe {
		if file.err != nil {
			return 0, file.err
		}
		return 0, newError(500, "Write on closed file")
	}
	if err == nil && file.checkHash {
		_, _ = file.hash.Write(p)
	}
	return
}

// Close the object and checks the hash if it was required.
//
// Also returns any other errors from the server (eg container not
// found) so it is very important to check the errors on this method.
func (file *ObjectCreateFile) Close() error {
	// Close the body
	err := file.pipeWriter.Close()
	if err != nil {
		return err
	}

	// Wait for the HTTP operation to complete
	<-file.done

	// Check errors
	if file.err != nil {
		return file.err
	}
	if file.checkHash {
		received := strings.ToLower(file.headers["Etag"])
		calculated := fmt.Sprintf("%x", file.hash.Sum(nil))
		if received != calculated {
			return ObjectCorrupted
		}
	}
	return nil
}

// Check it satisfies the interface
var _ io.WriteCloser = &ObjectCreateFile{}

// ------------------------------------------------------------

// ObjectPut creates or updates the path in the container from
// contents.  contents should be an open io.Reader which will have all
// its contents read.
//
// This is a low level interface.
//
// If checkHash is True then it will calculate the SHA1 Hash of the
// file as it is being uploaded and check it against that returned
// from the server.  If it is wrong then it will return
// ObjectCorrupted.
//
// If you don't want any error protection (not recommended) then set
// checkHash to false.
//
// If contentType is set it will be used, otherwise one will be
// guessed from objectName using mime.TypeByExtension
func (c *Connection) ObjectPut(container string, objectName string, contents io.Reader, checkHash bool, contentType string, h Headers) (headers Headers, err error) {
	extraHeaders := objectPutHeaders(objectName, contentType, h)
	hash := sha1.New()
	var body io.Reader = contents
	if checkHash {
		body = io.TeeReader(contents, hash)
	}
	_, headers, err = c.Call(HostUpload, RequestOpts{
		BucketName: container,
		ObjectName: objectName,
		Operation:  "PUT",
		Headers:    extraHeaders,
		Body:       body,
		NoResponse: true,
		ErrorMap:   objectErrorMap,
	})
	if err != nil {
		return
	}
	if checkHash {
		received := strings.ToLower(headers["Etag"])
		calculated := fmt.Sprintf("%x", hash.Sum(nil))
		if received != calculated {
			err = ObjectCorrupted
			return
		}
	}
	return
}

// objectPutHeaders create a set of headers for a PUT
//
// It guesses the contentType from the objectName if it isn't set
//
func objectPutHeaders(objectName string, contentType string, h Headers) Headers {
	if contentType == "" {
		contentType = mime.TypeByExtension(path.Ext(objectName))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
	}
	// Meta stuff
	extraHeaders := map[string]string{
		"Content-Type": contentType,
	}
	for key, value := range h {
		extraHeaders[key] = value
	}
	return extraHeaders
}

// ObjectCreate creates or updates the object in the container.  It
// returns an io.WriteCloser you should write the contents to.  You
// MUST call Close() on it and you MUST check the error return from
// Close().
//
// If checkHash is True then it will calculate the SHA1 Hash of the
// file as it is being uploaded and check it against that returned
// from the server.  If it is wrong then it will return
// ObjectCorrupted on Close()
//
// If you don't want any error protection (not recommended) then set
// checkHash to false.
//
// If contentType is set it will be used, otherwise one will be
// guessed from objectName using mime.TypeByExtension
func (c *Connection) ObjectCreate(container string, objectName string, checkHash bool, contentType string, h Headers) (file *ObjectCreateFile, err error) {
	extraHeaders := objectPutHeaders(objectName, contentType, h)
	pipeReader, pipeWriter := io.Pipe()
	file = &ObjectCreateFile{
		hash:       sha1.New(),
		checkHash:  checkHash,
		pipeReader: pipeReader,
		pipeWriter: pipeWriter,
		done:       make(chan struct{}),
	}
	// Run the PUT in the background piping it data
	go func() {
		file.resp, file.headers, file.err = c.Call(HostUpload, RequestOpts{
			BucketName: container,
			ObjectName: objectName,
			Operation:  "PUT",
			Headers:    extraHeaders,
			Body:       pipeReader,
			NoResponse: true,
			ErrorMap:   objectErrorMap,
		})
		// Signal finished
		pipeReader.Close()
		close(file.done)
	}()
	return
}

// ObjectPutBytes creates an object from a []byte in a container.
//
// This is a simplified interface which checks the SHA1.
func (c *Connection) ObjectPutBytes(container string, objectName string, contents []byte, contentType string) (err error) {
	buf := bytes.NewBuffer(contents)
	_, err = c.ObjectPut(container, objectName, buf, true, contentType, Headers{
		"Content-Length": strconv.Itoa(len(contents)),
	})
	return
}

// ObjectPutString creates an object from a string in a container.
//
// This is a simplified interface which checks the SHA1
func (c *Connection) ObjectPutString(container string, objectName string, contents string, contentType string) (err error) {
	buf := strings.NewReader(contents)
	_, err = c.ObjectPut(container, objectName, buf, true, contentType, Headers{
		"Content-Length": strconv.Itoa(len(contents)),
	})
	return
}

// -------------------------------------------------------------------

// Object contains information about an object
type Object struct {
	Name               string    // object name
	ContentType        string    // eg application/directory
	Bytes              int64     // size in bytes
	ServerLastModified string    // Last modified time, eg '2011-06-30T08:20:47.736680' as a string supplied by the server
	LastModified       time.Time // Last modified time converted to a time.Time
	Hash               string    // SHA1 hash, eg "7e240de74fb1ed08fa08d38063f6a6a91462a815"
}

// Object returns info about a single object including any metadata in the header.
//
// May return ObjectNotFound.
//
func (c *Connection) Object(container string, objectName string) (info Object, headers Headers, err error) {
	var resp *http.Response
	resp, headers, err = c.Call(Host, RequestOpts{
		BucketName: container,
		ObjectName: objectName,
		Operation:  "HEAD",
		ErrorMap:   objectErrorMap,
		NoResponse: true,
	})
	if err != nil {
		return
	}
	// Parse the headers into the struct
	// HTTP/1.1 200 OK
	// Content-Type: application/octet-stream
	// Expires: Wed, 11 Nov 2015 07:46:05 GMT
	// Last-Modified: Tue, 10 Nov 2015 07:36:03 GMT
	// Etag: 7e240de74fb1ed08fa08d38063f6a6a91462a815
	// Accept-Ranges: bytes
	// Cache-Control: public,max-age=86400
	// Server: nginx
	// Date: Tue, 10 Nov 2015 07:46:05 GMT
	// Content-Length: 3
	// Connection: keep-alive
	info.Name = objectName
	info.ContentType = resp.Header.Get("Content-Type")
	if resp.Header.Get("Content-Length") != "" {
		if info.Bytes, err = getInt64FromHeader(resp, "Content-Length"); err != nil {
			return
		}
	}
	info.ServerLastModified = resp.Header.Get("Last-Modified")
	if info.LastModified, err = time.Parse(http.TimeFormat, info.ServerLastModified); err != nil {
		return
	}
	info.Hash = resp.Header.Get("Etag")
	return
}

// getInt64FromHeader is a helper function to decode int64 from header.
func getInt64FromHeader(resp *http.Response, header string) (result int64, err error) {
	value := resp.Header.Get(header)
	result, err = strconv.ParseInt(value, 10, 64)
	if err != nil {
		err = newErrorf(0, "Bad Header '%s': '%s': %s", header, value, err)
	}
	return
}

// -------------------------------------------------------------------

// ObjectOpenFile represents a swift object open for reading
type ObjectOpenFile struct {
	connection *Connection    // stored copy of Connection used in Open
	container  string         // stored copy of container used in Open
	objectName string         // stored copy of objectName used in Open
	headers    Headers        // stored copy of headers used in Open
	resp       *http.Response // http connection
	body       io.Reader      // read data from this
	checkHash  bool           // true if checking SHA1
	hash       hash.Hash      // currently accumulating SHA1
	bytes      int64          // number of bytes read on this connection
	eof        bool           // whether we have read end of file
	pos        int64          // current position when reading
	lengthOk   bool           // whether length is valid
	length     int64          // length of the object if read
	seeked     bool           // whether we have seeked this file or not
}

// Read bytes from the object - see io.Reader
func (file *ObjectOpenFile) Read(p []byte) (n int, err error) {
	n, err = file.body.Read(p)
	file.bytes += int64(n)
	file.pos += int64(n)
	if err == io.EOF {
		file.eof = true
	}
	return
}

// Seek sets the offset for the next Read to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1
// means relative to the current offset, and 2 means relative to the
// end. Seek returns the new offset and an Error, if any.
//
// Seek uses HTTP Range headers which, if the file pointer is moved,
// will involve reopening the HTTP connection.
//
// Note that you can't seek to the end of a file or beyond; HTTP Range
// requests don't support the file pointer being outside the data,
// unlike os.File
//
// Seek(0, 1) will return the current file pointer.
func (file *ObjectOpenFile) Seek(offset int64, whence int) (newPos int64, err error) {
	switch whence {
	case 0: // relative to start
		newPos = offset
	case 1: // relative to current
		newPos = file.pos + offset
	case 2: // relative to end
		if !file.lengthOk {
			return file.pos, newError(0, "Length of file unknown so can't seek from end")
		}
		newPos = file.length + offset
	default:
		panic("Unknown whence in ObjectOpenFile.Seek")
	}
	// If at correct position (quite likely), do nothing
	if newPos == file.pos {
		return
	}
	// Close the file...
	file.seeked = true
	err = file.Close()
	if err != nil {
		return
	}
	// ...and re-open with a Range header
	if file.headers == nil {
		file.headers = Headers{}
	}
	if newPos > 0 {
		file.headers["Range"] = fmt.Sprintf("bytes=%d-", newPos)
	} else {
		delete(file.headers, "Range")
	}
	newFile, _, err := file.connection.ObjectOpen(file.container, file.objectName, false, file.headers)
	if err != nil {
		return
	}
	// Update the file
	file.resp = newFile.resp
	file.body = newFile.body
	file.checkHash = false
	file.pos = newPos
	return
}

// Length gets the objects content length either from a cached copy or
// from the server.
func (file *ObjectOpenFile) Length() (int64, error) {
	if !file.lengthOk {
		info, _, err := file.connection.Object(file.container, file.objectName)
		file.length = info.Bytes
		file.lengthOk = (err == nil)
		return file.length, err
	}
	return file.length, nil
}

// Close the object and checks the length and sha1sum if it was
// required and all the object was read
func (file *ObjectOpenFile) Close() (err error) {
	// Close the body at the end
	defer checkClose(file.resp.Body, &err)

	// If not end of file or seeked then can't check anything
	if !file.eof || file.seeked {
		return
	}

	// Check the SHA1 sum if requested
	if file.checkHash {
		received := strings.ToLower(file.resp.Header.Get("Etag"))
		calculated := fmt.Sprintf("%x", file.hash.Sum(nil))
		if received != calculated {
			err = ObjectCorrupted
			return
		}
	}

	// Check to see we read the correct number of bytes
	if file.lengthOk && file.length != file.bytes {
		err = ObjectCorrupted
		return
	}
	return
}

// Check it satisfies the interfaces
var _ io.ReadCloser = &ObjectOpenFile{}
var _ io.Seeker = &ObjectOpenFile{}

// checkClose is used to check the return from Close in a defer
// statement.
func checkClose(c io.Closer, err *error) {
	cerr := c.Close()
	if *err == nil {
		*err = cerr
	}
}

// ObjectOpen returns an ObjectOpenFile for reading the contents of
// the object.  This satisfies the io.ReadCloser and the io.Seeker
// interfaces.
//
// You must call Close() on contents when finished
//
// Returns the headers of the response.
//
// If checkHash is true then it will calculate the sha1sum of the file
// as it is being received and check it against that returned from the
// server.  If it is wrong then it will return ObjectCorrupted. It
// will also check the length returned. No checking will be done if
// you don't read all the contents.
//
// headers["Content-Type"] will give the content type if desired.
func (c *Connection) ObjectOpen(container string, objectName string, checkHash bool, h Headers) (file *ObjectOpenFile, headers Headers, err error) {
	var resp *http.Response
	resp, headers, err = c.Call(HostDownload, RequestOpts{
		BucketName: container,
		ObjectName: objectName,
		Operation:  "GET",
		ErrorMap:   objectErrorMap,
		Headers:    h,
	})
	if err != nil {
		return
	}
	file = &ObjectOpenFile{
		connection: c,
		container:  container,
		objectName: objectName,
		headers:    h,
		resp:       resp,
		checkHash:  checkHash,
		body:       resp.Body,
	}
	if checkHash {
		file.hash = sha1.New()
		file.body = io.TeeReader(resp.Body, file.hash)
	}
	// Read Content-Length
	if resp.Header.Get("Content-Length") != "" {
		file.length, err = getInt64FromHeader(resp, "Content-Length")
		file.lengthOk = (err == nil)
	}
	return
}

// ObjectGet gets the object into the io.Writer contents.
//
// Returns the headers of the response.
//
// If checkHash is true then it will calculate the sha1sum of the file
// as it is being received and check it against that returned from the
// server.  If it is wrong then it will return ObjectCorrupted.
//
// headers["Content-Type"] will give the content type if desired.
func (c *Connection) ObjectGet(container string, objectName string, contents io.Writer, checkHash bool, h Headers) (headers Headers, err error) {
	file, headers, err := c.ObjectOpen(container, objectName, checkHash, h)
	if err != nil {
		return
	}
	defer checkClose(file, &err)
	_, err = io.Copy(contents, file)
	return
}

// ObjectGetBytes returns an object as a []byte.
//
// This is a simplified interface which checks the SHA1
func (c *Connection) ObjectGetBytes(container string, objectName string) (contents []byte, err error) {
	var buf bytes.Buffer
	_, err = c.ObjectGet(container, objectName, &buf, true, nil)
	contents = buf.Bytes()
	return
}

// ObjectGetString returns an object as a string.
//
// This is a simplified interface which checks the SHA1
func (c *Connection) ObjectGetString(container string, objectName string) (contents string, err error) {
	var buf bytes.Buffer
	_, err = c.ObjectGet(container, objectName, &buf, true, nil)
	contents = buf.String()
	return
}

// ------------------------------------------------------------------

// ObjectTempUrl returns a temporary URL for an object
func (c *Connection) ObjectTempUrl(container string, objectName string, method string, expires int64) (URLs []string, err error) {
	var resp *http.Response
	resp, _, err = c.Call(HostDownload, RequestOpts{
		BucketName: container,
		ObjectName: objectName,
		Operation:  "GET",
		ErrorMap:   objectErrorMap,
		Parameters: url.Values{
			"filelocation": nil,
		},
		Expires: expires,
	})
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var aList []string
	if err = json.NewDecoder(resp.Body).Decode(&aList); err != nil {
		return
	}
	token, err := c.GetAuthToken(method, container, objectName, expires)
	if err != nil {
		return
	}
	for _, path := range aList {
		URLs = append(URLs, fmt.Sprintf("http://%s?token=%s", path, token))
	}
	return
}

// ObjectDelete deletes the object.
//
// May return ObjectNotFound if the object isn't found
func (c *Connection) ObjectDelete(container string, objectName string) error {
	_, _, err := c.Call(Host, RequestOpts{
		BucketName: container,
		ObjectName: objectName,
		Operation:  "DELETE",
		ErrorMap:   objectErrorMap,
	})
	return err
}

// ObjectCopy does a server side copy of an object to a new position
//
// All metadata is preserved.  If metadata is set in the headers then
// it overrides the old metadata on the copied object.
//
// The destination container must exist before the copy.
//
// You can use this to copy an object to itself - this is the only way
// to update the content type of an object.
func (c *Connection) ObjectCopy(srcContainer string, srcObjectName string, dstContainer string, dstObjectName string, h Headers) (headers Headers, err error) {
	extraHeaders := map[string]string{
		"x-bs2-copy-source": "/" + srcContainer + "/" + srcObjectName,
	}
	for key, value := range h {
		extraHeaders[key] = value
	}
	_, headers, err = c.Call(HostUpload, RequestOpts{
		BucketName: dstContainer,
		ObjectName: dstObjectName,
		Operation:  "PUT",
		ErrorMap:   objectErrorMap,
		NoResponse: true,
		Headers:    extraHeaders,
	})
	return
}

// ObjectMove does a server side move of an object to a new position
//
// All metadata is preserved.
//
// The destination container must exist before the move.
func (c *Connection) ObjectMove(srcContainer string, srcObjectName string, dstContainer string, dstObjectName string, h Headers) (headers Headers, err error) {
	extraHeaders := map[string]string{
		"x-bs2-move-source": "/" + srcContainer + "/" + srcObjectName,
	}
	for key, value := range h {
		extraHeaders[key] = value
	}
	_, headers, err = c.Call(HostUpload, RequestOpts{
		BucketName: dstContainer,
		ObjectName: dstObjectName,
		Operation:  "PUT",
		ErrorMap:   objectErrorMap,
		NoResponse: true,
		Headers:    extraHeaders,
	})
	return
}
