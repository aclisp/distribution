package bs2

import (
	"testing"

	"bytes"
	"fmt"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	"gopkg.in/check.v1"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

var bs2DriverConstructor func() (*Driver, error)
var skipBS2 func() string

func init() {
	runtime.GOMAXPROCS(4)

	bs2DriverConstructor = func() (*Driver, error) {
		parameters := DriverParameters{
			AccessKey: "ak_tqo",
			SecretKey: "78f372edb18b8c803b3192fbd441880f96cd7dfe",
			Bucket:    "sigmalargeimages",
			ChunkSize: defaultChunkSize,
		}

		return New(parameters), nil
	}

	testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return bs2DriverConstructor()
	}, testsuites.NeverSkip)
}

func testBasic(t *testing.T) {
	tf, err := ioutil.TempFile("", "tf")
	if err != nil {
		t.Fatalf("Can not create temp file: %s", err)
	}
	defer os.Remove(tf.Name())
	defer tf.Close()
	fmt.Printf("Temp file is %s\n", tf.Name())

	driver, err := factory.Create(driverName, map[string]interface{}{
		"accesskey": "ak_tqo",
		"secretkey": "78f372edb18b8c803b3192fbd441880f96cd7dfe",
		"bucket":    "sigmalargeimages",
	})
	if err != nil {
		t.Fatalf("Can not create driver from factory: %s", err)
	}

	err = driver.PutContent(nil, "/a", []byte("hello world!"))
	if err != nil {
		t.Fatalf("Can not put content: %s", err)
	}

	out, err := driver.GetContent(nil, "/a")
	if err != nil {
		t.Fatalf("Can not get content: %s", err)
	}
	if string(out) != "hello world!" {
		t.Fatalf("What I get is not the same as what I put!")
	}

	stream, err := driver.ReadStream(nil, "/a", 0)
	if err != nil {
		t.Fatalf("Can not read stream: %s", err)
	}
	defer stream.Close()
	out, err = ioutil.ReadAll(stream)
	if err != nil {
		t.Fatalf("Can not read all from stream: %s", err)
	}
	if string(out) != "hello world!" {
		t.Fatalf("What I get is not the same as what I put! got=%s", out)
	}

	stream, err = driver.ReadStream(nil, "/a", 11)
	if err != nil {
		t.Fatalf("Can not read stream: %s", err)
	}
	defer stream.Close()
	out, err = ioutil.ReadAll(stream)
	if err != nil {
		t.Fatalf("Can not read all from stream: %s", err)
	}
	if string(out) != "!" {
		t.Fatalf("What I get is not the same as what I put!")
	}

	fmt.Println("Write stream with offset 0")
	nRead, err := driver.WriteStream(nil, "/a/b/1", 0, strings.NewReader("12345678901234567890"))
	if err != nil {
		t.Fatalf("Can not write stream: %s", err)
	}
	if nRead != 20 {
		t.Fatalf("nRead is wrong: it is %d", nRead)
	}

	fmt.Println("Write stream with offset 20")
	nRead, err = driver.WriteStream(nil, "/a/b/1", 20, strings.NewReader("12345678901234567890"))
	if err != nil {
		t.Fatalf("Can not write stream: %s", err)
	}
	if nRead != 20 {
		t.Fatalf("nRead is wrong: it is %d", nRead)
	}

	// Test re-writing the last chunk
	fmt.Println("Write stream with offset 20")
	nRead, err = driver.WriteStream(nil, "/a/b/1", 20, strings.NewReader("12345678901234567890"))
	if err != nil {
		t.Fatalf("Can not write stream: %s", err)
	}
	if nRead != 20 {
		t.Fatalf("nRead is wrong: it is %d", nRead)
	}

	stream, err = driver.ReadStream(nil, "/a/b/1", 0)
	if err != nil {
		t.Fatalf("Can not get content: %s", err)
	}
	defer stream.Close()
	out, err = ioutil.ReadAll(stream)
	if err != nil {
		t.Fatalf("Can not read all from stream: %s", err)
	}
	if string(out) != "1234567890123456789012345678901234567890" {
		t.Fatalf("What I get is not the same as what I put! got=%s", out)
	}

	// Writing past size of file extends file (no offset error).
	fmt.Println("Write stream with offset 60")
	nRead, err = driver.WriteStream(nil, "/a/b/1", 60, strings.NewReader("12345678901234567890"))
	if err != nil {
		t.Fatalf("Can not write stream: %s", err)
	}
	if nRead != 20 {
		t.Fatalf("nRead is wrong: it is %d", nRead)
	}

	stream, err = driver.ReadStream(nil, "/a/b/1", 0)
	if err != nil {
		t.Fatalf("Can not get content: %s", err)
	}
	defer stream.Close()
	out, err = ioutil.ReadAll(stream)
	if err != nil {
		t.Fatalf("Can not read all from stream: %s", err)
	}
	if string(out[:40]) != "1234567890123456789012345678901234567890" {
		t.Fatalf("What I get is not the same as what I put! got=%v", out)
	}
	if bytes.Compare(out[40:60], make([]byte, 20)) != 0 {
		t.Fatalf("What I get is not the same as what I put! got=%v", out)
	}
	if string(out[60:]) != "12345678901234567890" {
		t.Fatalf("What I get is not the same as what I put! got=%v", out)
	}
}
