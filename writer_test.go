package snappyframed

import (
	"bytes"
	"io/ioutil"
	"log"
	"testing"
)

// This test ensures that all Writer methods fail after Close has been
// called.
func TestWriterClose(t *testing.T) {
	w := NewWriter(ioutil.Discard)
	err := w.Close()
	if err != nil {
		log.Fatalf("closing empty Writer: %v", err)
	}
	err = w.Close()
	if err == nil {
		log.Fatalf("successful close after close")
	}
	err = w.Flush()
	if err == nil {
		log.Fatalf("successful flush after close")
	}
	_, err = w.Write([]byte("abc"))
	if err == nil {
		log.Fatalf("successful write after close")
	}
}

// This test simply checks that buffering has an effect in a situation where it
// is know it should.
func TestWriter_compression(t *testing.T) {
	p := []byte("hello snappystream!")
	n := 10

	var shortbuf bytes.Buffer
	w := newWriter(&shortbuf)
	for i := 0; i < n; i++ {
		n, err := w.Write(p)
		if err != nil {
			t.Fatalf("writer error: %v", err)
		}
		if n != len(p) {
			t.Fatalf("short write: %d", n)
		}
	}

	var buf bytes.Buffer
	bw := NewWriter(&buf)
	for i := 0; i < n; i++ {
		n, err := bw.Write(p)
		if err != nil {
			t.Fatalf("buffered writer error: %v", err)
		}
		if n != len(p) {
			t.Fatalf("short write: %d", n)
		}
	}
	err := bw.Close()
	if err != nil {
		t.Fatalf("closing buffer: %v", err)
	}

	uncompressed := int64(n) * int64(len(p))
	compressed := shortbuf.Len()
	bufcompressed := buf.Len()

	if compressed <= bufcompressed {
		t.Fatalf("no benefit from buffering (%d <= %d)", shortbuf.Len(), buf.Len())
	}

	c := float64(uncompressed) / float64(compressed)
	bufc := float64(uncompressed) / float64(bufcompressed)
	improved := bufc / c

	t.Logf("Writer compression ratio %g (%.03g factor improvement over %g)", bufc, improved, c)
}

// This tests ensures flushing after every write is equivalent to using
// NewWriter directly.
func TestWriterFlush(t *testing.T) {
	p := []byte("hello snappystream!")
	n := 10

	var shortbuf bytes.Buffer
	w := newWriter(&shortbuf)
	for i := 0; i < n; i++ {
		n, err := w.Write(p)
		if err != nil {
			t.Fatalf("writer error: %v", err)
		}
		if n != len(p) {
			t.Fatalf("short write: %d", n)
		}
	}

	var buf bytes.Buffer
	bw := NewWriter(&buf)
	for i := 0; i < n; i++ {
		n, err := bw.Write(p)
		if err != nil {
			t.Fatalf("buffered writer error: %v", err)
		}
		if n != len(p) {
			t.Fatalf("short write: %d", n)
		}
		err = bw.Flush()
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	err := bw.Close()
	if err != nil {
		t.Fatalf("closing buffer: %v", err)
	}

	if shortbuf.Len() != buf.Len() {
		t.Fatalf("unexpected size: %d != %d", shortbuf.Len(), buf.Len())
	}

	if !bytes.Equal(shortbuf.Bytes(), buf.Bytes()) {
		t.Fatalf("unexpected bytes")
	}
}

func TestWriterReset(t *testing.T) {
	data := []byte("hello reset")
	var buf1 bytes.Buffer
	var buf2 bytes.Buffer

	w := NewWriter(&buf1)
	_, err := w.Write(data)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	w.Reset(&buf2)
	_, err = w.Write(data)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	b1 := buf1.Bytes()
	b2 := buf2.Bytes()

	if len(b1) != 0 {
		t.Errorf("buffer 1: %q", b1)
	}

	b2r := NewReader(bytes.NewReader(b2))
	b2dec, err := ioutil.ReadAll(b2r)
	if err != nil {
		t.Errorf("buffer 2: %v", err)
	} else if !bytes.Equal(b2dec, data) {
		t.Errorf("buffer 2: %q", b2)
	}
}
