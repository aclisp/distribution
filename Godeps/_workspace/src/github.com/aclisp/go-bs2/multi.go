package bs2

import (
	"crypto/sha1"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Multi represents an unfinished multipart upload.
//
// Multipart uploads allow sending big objects in smaller chunks.
// After all parts have been sent, the upload must be explicitly
// completed by calling Complete with the list of parts.
//
type Multi struct {
	Conn       *Connection
	BucketName string
	ObjectName string
	UploadId   string    // identify this multi-part upload activity
	ZoneName   string    // used as the host to which the following parts upload
	Uploading  bool      // indicate whether upload is on going
	checkHash  bool      // whether we are checking the hash
	hash       hash.Hash // hash being build up as we go along
}

type UploadStats string

const (
	UploadError      = ""
	UploadNotStart   = "-"
	UploadComplete   = "0"
	UploadInProgress = "2"
)

type LastPart struct {
	XMLName     xml.Name
	BucketName  string `xml:"bucket"`
	ObjectName  string `xml:"filename"`
	UploadId    string `xml:"uploadid"`
	PartNumber  string `xml:"partnumber"`
	CurrentSize string `xml:"currentsize"`
}

type Part struct {
	N    int
	Size int64
}

// InitMulti initializes a new multipart upload at the provided
// object inside the container and returns a value for manipulating it.
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
func (c *Connection) InitMulti(container string, objectName string, checkHash bool, contentType string) (*Multi, error) {
	extraHeaders := objectPutHeaders(objectName, contentType, nil)
	resp, _, err := c.Call(HostUpload, RequestOpts{
		BucketName: container,
		ObjectName: objectName,
		Operation:  "POST",
		Parameters: url.Values{
			"uploads": nil,
		},
		Headers:  extraHeaders,
		ErrorMap: objectErrorMap,
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var respContent struct {
		Zone     string
		UploadId string
	}
	if err = json.NewDecoder(resp.Body).Decode(&respContent); err != nil {
		return nil, err
	}
	if respContent.Zone == "" || respContent.UploadId == "" {
		return nil, errors.New("Multi-part uploads initialize: got null Zone or UploadId")
	}
	return &Multi{
		Conn:       c,
		BucketName: container,
		ObjectName: objectName,
		UploadId:   respContent.UploadId,
		ZoneName:   removeBucketNameFromZone(respContent.Zone, container),
		Uploading:  true,
		checkHash:  checkHash,
		hash:       sha1.New(),
	}, nil
}

func removeBucketNameFromZone(zone string, bucketName string) string {
	return zone[len(bucketName)+1:]
}

// Multi returns a multipart upload handler for the provided object
// inside the container. If a multipart upload exists for the object, it is returned.
//
func (c *Connection) Multi(container string, objectName string) (*Multi, error) {
	resp, _, err := c.Call(Host, RequestOpts{
		Operation: "GET",
		ErrorMap:  objectErrorMap,
		Parameters: url.Values{
			"filelocation": nil,
			"bucket":       {container},
			"file":         {objectName},
		},
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	type Location struct {
		ZoneId  string `xml:"zoneid"`
		ZoneUrl string `xml:"zoneurl"`
	}
	type Result struct {
		XMLName  xml.Name
		Location []Location `xml:"location"`
	}
	var result Result
	if err = xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if len(result.Location) == 0 {
		return nil, errors.New("Multi-part uploads retrieve: got null file location")
	}
	uploadId, uploading, err := uploadIdFromUrl(result.Location[0].ZoneUrl)
	if err != nil {
		return nil, err
	}
	if uploadId == "" {
		return nil, errors.New("Multi-part uploads retrieve: got null upload id")
	}
	return &Multi{
		Conn:       c,
		BucketName: container,
		ObjectName: objectName,
		UploadId:   uploadId,
		ZoneName:   result.Location[0].ZoneId + "." + Host,
		Uploading:  uploading,
	}, nil
}

func uploadIdFromUrl(rawUrl string) (uploadId string, uploading bool, err error) {
	var URL *url.URL
	if URL, err = url.Parse(rawUrl); err != nil {
		return
	}
	var values url.Values
	if values, err = url.ParseQuery(URL.RawQuery); err != nil {
		return
	}
	uploadId = values.Get("uploadid")
	uploading = values.Get("uploading") == "1"
	return
}

func (c *Connection) ResumeMulti(container string, objectName string) (*Multi, error) {
	resp, _, err := c.Call(Host, RequestOpts{
		BucketName: container,
		ObjectName: objectName,
		Operation:  "GET",
		ErrorMap:   objectErrorMap,
		Parameters: url.Values{
			"uploadstatus": nil,
			"bucket":       {container},
		},
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	fmt.Printf("ResumeMulti: %s", string(out))
	return nil, nil
}

// ------------------------------------------------------------------

func (m *Multi) Stats() (UploadStats, error) {
	resp, _, err := m.Conn.Call(HostDownload, RequestOpts{
		BucketName: m.BucketName,
		ObjectName: m.ObjectName,
		Operation:  "GET",
		Parameters: url.Values{
			"fileinformation": nil,
		},
		ErrorMap: objectErrorMap,
	})
	if err != nil {
		if err == ObjectNotFound {
			return UploadNotStart, err
		}
		return UploadError, err
	}
	defer resp.Body.Close()
	var respContent struct {
		FileStatus string
	}
	if err = json.NewDecoder(resp.Body).Decode(&respContent); err != nil {
		return UploadError, err
	}
	if respContent.FileStatus == "" {
		return UploadError, errors.New("Multi-part uploads status: got null FileStatus")
	}
	switch respContent.FileStatus {
	case UploadInProgress:
		m.Uploading = true
	case UploadComplete:
		m.Uploading = false
	default:
		return UploadError, fmt.Errorf("Multi-part uploads status: unrecognized file status: %s", respContent.FileStatus)
	}
	return UploadStats(respContent.FileStatus), nil
}

func (m *Multi) LastPart() (lastPart LastPart, err error) {
	var resp *http.Response
	resp, _, err = m.Conn.Call(m.ZoneName, RequestOpts{
		BucketName: m.BucketName,
		ObjectName: m.ObjectName,
		Operation:  "GET",
		Parameters: url.Values{
			"getlastpart": nil,
			"uploadid":    {m.UploadId},
			"bucket":      {m.BucketName},
		},
		ErrorMap: objectErrorMap,
	})
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if err = xml.NewDecoder(resp.Body).Decode(&lastPart); err != nil {
		return
	}
	if lastPart.UploadId == "" || lastPart.PartNumber == "" || lastPart.CurrentSize == "" {
		return lastPart, errors.New("Multi-part uploads get last part: got null fields")
	}
	return
}

// PutPart sends part n of the multipart upload, reading all the content from r.
// Each part, except for the last one, must be at least 100KB in size, but no
// more than 2MB. n starts from 0.
//
func (m *Multi) PutPart(n int, r io.ReadSeeker) (part Part, err error) {
	partSize, err := seekerInfo(r)
	if err != nil {
		return
	}
	_, err = r.Seek(0, 0)
	if err != nil {
		return
	}
	return m.PutSizedPart(n, r, partSize)
}

func (m *Multi) PutSizedPart(n int, r io.Reader, partSize int64) (part Part, err error) {
	if m.checkHash {
		r = io.TeeReader(r, m.hash)
	}
	_, _, err = m.Conn.Call(m.ZoneName, RequestOpts{
		BucketName: m.BucketName,
		ObjectName: m.ObjectName,
		Operation:  "PUT",
		Headers: Headers{
			"Content-Length": strconv.FormatInt(partSize, 10),
		},
		Parameters: url.Values{
			"partnumber": {strconv.Itoa(n)},
			"uploadid":   {m.UploadId},
		},
		Body:       r,
		NoResponse: true,
		ErrorMap:   objectErrorMap,
	})
	if err != nil {
		return
	}
	part = Part{n, partSize}
	return
}

func seekerInfo(r io.ReadSeeker) (size int64, err error) {
	_, err = r.Seek(0, 0)
	if err != nil {
		return
	}
	digest := nopWriter{}
	size, err = io.Copy(digest, r)
	if err != nil {
		return
	}
	return
}

type nopWriter struct {
}

func (nopWriter) Write(p []byte) (n int, err error) {
	// Do nothing
	return len(p), nil
}

// Complete indicates the given previously uploaded parts done.
//
func (m *Multi) Complete(total int) error {
	content := fmt.Sprintf(`{"partcount": "%d"}`, total)
	_, headers, err := m.Conn.Call(m.ZoneName, RequestOpts{
		BucketName: m.BucketName,
		ObjectName: m.ObjectName,
		Operation:  "POST",
		Headers: Headers{
			"Content-Length": strconv.Itoa(len(content)),
		},
		Parameters: url.Values{
			"uploadid": {m.UploadId},
		},
		Body:       strings.NewReader(content),
		NoResponse: true,
		ErrorMap:   objectErrorMap,
	})
	if err != nil {
		return err
	}
	if m.checkHash {
		received := strings.ToLower(headers["Etag"])
		calculated := fmt.Sprintf("%x", m.hash.Sum(nil))
		if received != calculated {
			return ObjectCorrupted
		}
	}
	return nil
}
