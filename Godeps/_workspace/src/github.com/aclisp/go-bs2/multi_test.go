package bs2_test

import (
	//"fmt"
	"github.com/aclisp/go-bs2"
	"strconv"
	"strings"
	"testing"
)

func TestInitMulti(t *testing.T) {
	c := bs2.Connection{
		AccessKey: a,
		SecretKey: s,
		Logger:    nil,
	}

	multi, e := c.InitMulti(myLargeBucket, myLargeObj, true, "")
	if e != nil {
		t.Fatal(e)
	}
	//fmt.Printf("multi: %#v\n\n", multi)

	//var status bs2.UploadStats
	//status, e = multi.Stats()
	//fmt.Printf("status: %#v, error: %#v\n\n", status, e)
	//if status != bs2.UploadNotStart || e != bs2.ObjectNotFound {
	//	t.Fatal(e)
	//}

	var lastpart bs2.LastPart
	lastpart, e = multi.LastPart()
	//fmt.Printf("lastpart: %#v, error: %#v\n\n", lastpart, e)
	if lastpart.UploadId != multi.UploadId || lastpart.PartNumber != "NAN" || lastpart.CurrentSize != "NAN" || e != nil {
		t.Fatal(e)
	}

	var i int
	for i = 0; i < 10; i++ {
		putPart(multi, i, t)
	}

	//putPart(multi, 9, t)

	e = multi.Complete(i)
	if e != nil {
		t.Fatal(e)
	}

	lastpart, e = multi.LastPart()
	//fmt.Printf("finalpart: %#v, error: %#v\n\n", lastpart, e)
	if lastpart.UploadId != multi.UploadId || lastpart.PartNumber != "NAN" || lastpart.CurrentSize != "NAN" || e != nil {
		t.Fatal(e)
	}

	var status bs2.UploadStats
	status, e = multi.Stats()
	//fmt.Printf("finalstatus: %#v, error: %#v\n\n", status, e)
	if status != bs2.UploadComplete || e != nil {
		t.Fatal(e)
	}

	testMulti(t)
}

func putPart(multi *bs2.Multi, n int, t *testing.T) {
	_, e := multi.PutPart(n, strings.NewReader(myLargeObjPart))
	if e != nil {
		t.Fatal(e)
	}
	//fmt.Printf("part: %#v\n\n", part)

	//var status bs2.UploadStats
	//status, e = multi.Stats()
	//fmt.Printf("status: %#v, error: %#v\n\n", status, e)
	//if status != bs2.UploadInProgress || e != nil {
	//	t.Fatal(e)
	//}

	var lastpart bs2.LastPart
	lastpart, e = multi.LastPart()
	//fmt.Printf("lastpart: %#v, error: %#v\n\n", lastpart, e)
	if lastpart.UploadId != multi.UploadId || lastpart.PartNumber != strconv.Itoa(n) || lastpart.CurrentSize != strconv.Itoa(len(myLargeObjPart)*(n+1)) || e != nil {
		t.Fatal(e)
	}
}

func testMulti(t *testing.T) {
	c := bs2.Connection{
		AccessKey: a,
		SecretKey: s,
		Logger:    nil,
	}

	multi, e := c.Multi(myLargeBucket, myLargeObj)
	if e != nil {
		t.Fatal(e)
	}
	//fmt.Printf("multi: %#v\n\n", multi)

	var status bs2.UploadStats
	status, e = multi.Stats()
	//fmt.Printf("status: %#v, error: %#v\n\n", status, e)
	if status != bs2.UploadComplete || e != nil {
		t.Fatal(e)
	}

	var lastpart bs2.LastPart
	lastpart, e = multi.LastPart()
	//fmt.Printf("lastpart: %#v, error: %#v\n\n", lastpart, e)
	if lastpart.UploadId != multi.UploadId || lastpart.PartNumber != "NAN" || lastpart.CurrentSize != "NAN" || e != nil {
		t.Fatal(e)
	}
}
