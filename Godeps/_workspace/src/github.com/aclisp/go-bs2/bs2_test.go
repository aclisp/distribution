package bs2_test

import (
	"flag"
	"fmt"
	"github.com/aclisp/go-bs2"
	//"io"
	"io/ioutil"
	"log"
	"net/http"
	//"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	myBucket   = "sigmalargeimages"
	myObj      = "myobj"
	myObjCopy  = "myobjcopy"
	myObjMove  = "myobjmove"
	myObjValue = "0123456789"
	myObjSha1  = "87acec17cd9dcd20a716cc2cf67417b71c8a7016"

	myLargeBucket  = "sigmalargeimages"
	myLargeObj     = "mylargeobj"
	myLargeObjPart = "9876543210"
)

var (
	logger = log.New(os.Stderr, "bs2: ", 0)
	a      = os.Getenv("BS2_ACCESS_KEY")
	s      = os.Getenv("BS2_SECRET_KEY")
)

func init() {
	if a == "" || s == "" {
		panic("Must set BS2_ACCESS_KEY and BS2_SECRET_KEY to run BS2 tests")
	}
}

func TestCall(t *testing.T) {
	c := bs2.Connection{
		AccessKey: a,
		SecretKey: s,
		Logger:    logger,
	}
	/*
		r, _, e := c.Call(bs2.Host, bs2.RequestOpts{
			BucketName: myBucket,
			ObjectName: myObj,
			Operation:  "GET",
			Parameters: url.Values{
			//"fileinformation": nil,
			},
		})
		if e != nil {
			t.Fatal(e)
		}
		io.Copy(os.Stdout, r.Body)
		fmt.Println()
		r.Body.Close()
	*/

	m, e := c.InitMulti(myLargeBucket, "hello5", true, "")
	if e != nil {
		t.Fatal(e)
	}
	fmt.Printf("%#v\n\n", m)

	part, e := m.PutPart(0, strings.NewReader("testString"))
	if e != nil {
		t.Fatal(e)
	}
	fmt.Printf("%#v\n\n", part)
	part, e = m.PutPart(1, strings.NewReader("testString"))
	if e != nil {
		t.Fatal(e)
	}
	fmt.Printf("%#v\n\n", part)

	//time.Sleep(2 * time.Second)

	m, e = c.Multi(myLargeBucket, "hello5")
	if e != nil {
		t.Fatal(e)
	}
	fmt.Printf("%#v\n\n", m)

}

func TestMain(m *testing.M) {
	flag.Parse()

	// setup the object for all tests
	c := bs2.Connection{
		AccessKey: a,
		SecretKey: s,
		Logger:    nil,
	}
	_, e := c.ObjectPut(myBucket, myObj, strings.NewReader(myObjValue), true, "", bs2.Headers{
		"Content-Length": strconv.Itoa(len(myObjValue)),
	})
	if e != nil {
		logger.Fatal("Tests setup failed")
	}

	exitCode := m.Run()

	// teardown
	e = c.ObjectDelete(myBucket, myObj)
	if e != nil {
		logger.Fatal("Tests teardown failed")
	}

	os.Exit(exitCode)
}

func TestObject(t *testing.T) {
	c := bs2.Connection{
		AccessKey: a,
		SecretKey: s,
		Logger:    nil,
	}
	o, _, e := c.Object(myBucket, myObj)
	if e != nil {
		t.Fatal(e)
	}
	if o.Name == "" || o.Bytes == 0 || o.ContentType == "" || o.Hash == "" || o.ServerLastModified == "" {
		t.Fatal("Some of the object fields are not set")
	}
	if o.Name != myObj {
		t.Fatal("Wrong object name")
	}
	if o.Bytes != int64(len(myObjValue)) {
		t.Fatal("Wrong object size")
	}
	if o.ContentType != "application/octet-stream" {
		t.Fatal("Wrong content type")
	}
	if o.Hash != myObjSha1 {
		t.Fatal("Wrong object content")
	}
}

func TestObjectCreate(t *testing.T) {
	c := bs2.Connection{
		AccessKey: a,
		SecretKey: s,
		Logger:    nil,
	}
	f, e := c.ObjectCreate(myBucket, myObj, true, "", bs2.Headers{
		"Content-Length": strconv.Itoa(len(myObjValue)),
	})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.Write([]byte(myObjValue)); e != nil {
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
}

func TestObjectPutBytes(t *testing.T) {
	c := bs2.Connection{
		AccessKey: a,
		SecretKey: s,
		Logger:    nil,
	}
	e := c.ObjectPutBytes(myBucket, myObj, []byte(myObjValue), "")
	if e != nil {
		t.Fatal(e)
	}
}

func TestObjectPutString(t *testing.T) {
	c := bs2.Connection{
		AccessKey: a,
		SecretKey: s,
		Logger:    nil,
	}
	e := c.ObjectPutString(myBucket, myObj, myObjValue, "")
	if e != nil {
		t.Fatal(e)
	}
}

func TestObjectGetString(t *testing.T) {
	c := bs2.Connection{
		AccessKey: a,
		SecretKey: s,
		Logger:    nil,
	}
	content, e := c.ObjectGetString(myBucket, myObj)
	if e != nil {
		t.Fatal(e)
	}
	if content != myObjValue {
		t.Fatal("Wrong object content: " + content)
	}
}

func TestObjectOpen(t *testing.T) {
	c := bs2.Connection{
		AccessKey: a,
		SecretKey: s,
		Logger:    nil,
	}
	file, _, e := c.ObjectOpen(myBucket, myObj, true, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer file.Close()
	file.Seek(1, 0)
	var part []byte
	if part, e = ioutil.ReadAll(file); e != nil {
		t.Fatal(e)
	}
	if string(part) != myObjValue[1:] {
		t.Fatal("Wrong object content after seek")
	}
}

func TestObjectTempUrl(t *testing.T) {
	c := bs2.Connection{
		AccessKey: a,
		SecretKey: s,
		Logger:    nil,
	}
	urls, e := c.ObjectTempUrl(myBucket, myObj, "GET", 900)
	if e != nil {
		t.Fatal(e)
	}
	for _, url := range urls {
		if str, err := httpGetString(url); err != nil || str != myObjValue {
			t.Fatal("URL does not work: " + url)
		}
	}
}

func httpGetString(url string) (result string, err error) {
	var resp *http.Response
	if resp, err = http.Get(url); err != nil {
		return
	}
	defer resp.Body.Close()
	var contents []byte
	if contents, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	return string(contents), nil
}

func TestObjectCopy(t *testing.T) {
	c := bs2.Connection{
		AccessKey: a,
		SecretKey: s,
		Logger:    nil,
	}
	if _, e := c.ObjectCopy(myBucket, myObj, myBucket, myObjCopy, nil); e != nil {
		t.Fatal(e)
	}
	if e := c.ObjectDelete(myBucket, myObjCopy); e != nil {
		t.Fatal(e)
	}
}

func TestObjectMove(t *testing.T) {
	c := bs2.Connection{
		AccessKey: a,
		SecretKey: s,
		Logger:    nil,
	}
	if _, e := c.ObjectCopy(myBucket, myObj, myBucket, myObjCopy, nil); e != nil {
		t.Fatal(e)
	}

	time.Sleep(2 * time.Second)

	if _, e := c.ObjectMove(myBucket, myObjCopy, myBucket, myObjMove, nil); e != nil {
		t.Fatal(e)
	}
	if e := c.ObjectDelete(myBucket, myObjMove); e != nil {
		t.Fatal(e)
	}
}
