package snappyframed

import (
	"bytes"
	"crypto/rand"
	"io"
	"io/ioutil"
	"sync"
	"testing"
)

const TestFileSize = 10 << 20 // 10MB

// dummyBytesReader returns an io.Reader that avoids buffering optimizations
// in io.Copy. This can be considered a 'worst-case' io.Reader as far as writer
// frame alignment goes.
//
// Note: io.Copy uses a 32KB buffer internally as of Go 1.3, but that isn't
// part of its public API (undocumented).
func dummyBytesReader(p []byte) io.Reader {
	return ioutil.NopCloser(bytes.NewReader(p))
}

func testWriteThenRead(t *testing.T, name string, bs []byte) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	n, err := io.Copy(w, dummyBytesReader(bs))
	if err != nil {
		t.Errorf("write %v: %v", name, err)
		return
	}
	if n != int64(len(bs)) {
		t.Errorf("write %v: wrote %d bytes (!= %d)", name, n, len(bs))
		return
	}
	err = w.Close()
	if err != nil {
		t.Errorf("close %v: %v", name, err)
		return
	}

	enclen := buf.Len()

	r := NewReader(&buf)
	gotbs, err := ioutil.ReadAll(r)
	if err != nil {
		t.Errorf("read %v: %v", name, err)
		return
	}
	n = int64(len(gotbs))
	if n != int64(len(bs)) {
		t.Errorf("read %v: read %d bytes (!= %d)", name, n, len(bs))
		return
	}

	if !bytes.Equal(gotbs, bs) {
		t.Errorf("%v: unequal decompressed content", name)
		return
	}

	c := float64(len(bs)) / float64(enclen)
	t.Logf("%v compression ratio %.03g (%d byte reduction)", name, c, len(bs)-enclen)
}

func TestWriterReader(t *testing.T) {
	testWriteThenRead(t, "simple", []byte("test"))
	testWriteThenRead(t, "manpage", testDataMan)
	testWriteThenRead(t, "json", testDataJSON)

	p := make([]byte, TestFileSize)
	testWriteThenRead(t, "constant", p)

	_, err := rand.Read(p)
	if err != nil {
		t.Fatal(err)
	}
	testWriteThenRead(t, "random", p)

}

func TestWriterChunk(t *testing.T) {
	var buf bytes.Buffer

	in := make([]byte, 128000)

	w := NewWriter(&buf)
	r := NewReader(&buf)

	n, err := w.Write(in)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if n != len(in) {
		t.Fatalf("wrote wrong amount %d != %d", n, len(in))
	}

	out := make([]byte, len(in))
	n, err = io.ReadFull(r, out)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(in) {
		t.Fatalf("read wrong amount %d != %d", n, len(in))
	}

	if !bytes.Equal(out, in) {
		t.Fatalf("bytes not equal %v != %v", out, in)
	}
}

func BenchmarkWriterManpage(b *testing.B) {
	benchmarkWriterBytes(b, testDataMan)
}
func BenchmarkWriterManpageNoCopy(b *testing.B) {
	benchmarkWriterBytesNoCopy(b, testDataMan)
}
func BenchmarkWriterManpageNoReset(b *testing.B) {
	benchmarkWriterBytesNoReset(b, testDataMan)
}
func BenchmarkWriterManpagePool(b *testing.B) {
	benchmarkWriterBytesPool(b, testDataMan)
}

func BenchmarkWriterJSON(b *testing.B) {
	benchmarkWriterBytes(b, testDataJSON)
}
func BenchmarkWriterJSONNoCopy(b *testing.B) {
	benchmarkWriterBytesNoCopy(b, testDataJSON)
}
func BenchmarkWriterJSONNoReset(b *testing.B) {
	benchmarkWriterBytesNoReset(b, testDataJSON)
}
func BenchmarkWriterJSONPool(b *testing.B) {
	benchmarkWriterBytesPool(b, testDataJSON)
}

// BenchmarkWriterRandom tests performance encoding effectively uncompressable
// data.
func BenchmarkWriterRandom(b *testing.B) {
	benchmarkWriterBytes(b, randBytes(b, TestFileSize))
}
func BenchmarkWriterRandomNoCopy(b *testing.B) {
	benchmarkWriterBytesNoCopy(b, randBytes(b, TestFileSize))
}
func BenchmarkWriterRandomNoReset(b *testing.B) {
	benchmarkWriterBytesNoReset(b, randBytes(b, TestFileSize))
}
func BenchmarkWriterRandomPool(b *testing.B) {
	benchmarkWriterBytesPool(b, randBytes(b, TestFileSize))
}

// BenchmarkWriterConstant tests performance encoding maximally compressible
// data.
func BenchmarkWriterConstant(b *testing.B) {
	benchmarkWriterBytes(b, make([]byte, TestFileSize))
}
func BenchmarkWriterConstantNoCopy(b *testing.B) {
	benchmarkWriterBytesNoCopy(b, make([]byte, TestFileSize))
}
func BenchmarkWriterConstantNoReset(b *testing.B) {
	benchmarkWriterBytesNoReset(b, make([]byte, TestFileSize))
}
func BenchmarkWriterConstantPool(b *testing.B) {
	benchmarkWriterBytesPool(b, make([]byte, TestFileSize))
}

func benchmarkWriterBytes(b *testing.B, p []byte) {
	w := NewWriter(ioutil.Discard)
	wcloser := &nopWriteCloser{w}
	enc := func() io.WriteCloser {
		// wrap the normal writer so that it has a noop Close method.  writer
		// does not implement ReaderFrom so this does not impact performance.
		w.Reset(ioutil.Discard)
		return wcloser
	}
	benchmarkEncode(b, enc, p)
}

func benchmarkWriterBytesNoCopy(b *testing.B, p []byte) {
	enc := func() io.WriteCloser {
		// the writer is wrapped as to hide it's ReaderFrom implemention.
		return &writeCloserNoCopy{NewWriter(ioutil.Discard)}
	}
	benchmarkEncode(b, enc, p)
}
func benchmarkWriterBytesNoReset(b *testing.B, p []byte) {
	enc := func() io.WriteCloser {
		// allocation is performed every iteration
		return NewWriter(ioutil.Discard)
	}
	benchmarkEncode(b, enc, p)
}
func benchmarkWriterBytesPool(b *testing.B, p []byte) {
	pool := &sync.Pool{
		New: func() interface{} {
			return NewWriter(ioutil.Discard)
		},
	}
	enc := func() io.WriteCloser {
		w := pool.Get().(*Writer)
		w.Reset(ioutil.Discard)
		return &poolWriter{pool, w}
	}
	benchmarkEncode(b, enc, p)
}

// benchmarkEncode benchmarks the speed at which bytes can be copied from
// bs into writers created by enc.
func benchmarkEncode(b *testing.B, enc func() io.WriteCloser, bs []byte) {
	size := int64(len(bs))
	b.SetBytes(size)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		w := enc()
		n, err := io.Copy(w, dummyBytesReader(bs))
		if err != nil {
			b.Fatal(err)
		}
		if n != size {
			b.Fatalf("wrote wrong amount %d != %d", n, size)
		}
		err = w.Close()
		if err != nil {
			b.Fatalf("close: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkReaderManpage(b *testing.B) {
	encodeAndBenchmarkReader(b, testDataMan)
}
func BenchmarkReaderManpageNoCopy(b *testing.B) {
	encodeAndBenchmarkReaderNoCopy(b, testDataMan)
}
func BenchmarkReaderManpageNoReset(b *testing.B) {
	encodeAndBenchmarkReaderNoReset(b, testDataMan)
}
func BenchmarkReaderManpagePool(b *testing.B) {
	encodeAndBenchmarkReaderPool(b, testDataMan)
}

func BenchmarkReaderJSON(b *testing.B) {
	encodeAndBenchmarkReader(b, testDataJSON)
}
func BenchmarkReaderJSONNoCopy(b *testing.B) {
	encodeAndBenchmarkReaderNoCopy(b, testDataJSON)
}
func BenchmarkReaderJSONNoReset(b *testing.B) {
	encodeAndBenchmarkReaderNoReset(b, testDataJSON)
}
func BenchmarkReaderJSONPool(b *testing.B) {
	encodeAndBenchmarkReaderPool(b, testDataJSON)
}

// BenchmarkReaderRandom tests decoding of effectively uncompressable data.
func BenchmarkReaderRandom(b *testing.B) {
	encodeAndBenchmarkReader(b, randBytes(b, TestFileSize))
}
func BenchmarkReaderRandomNoCopy(b *testing.B) {
	encodeAndBenchmarkReaderNoCopy(b, randBytes(b, TestFileSize))
}
func BenchmarkReaderRandomNoReset(b *testing.B) {
	encodeAndBenchmarkReaderNoReset(b, randBytes(b, TestFileSize))
}
func BenchmarkReaderRandomPool(b *testing.B) {
	encodeAndBenchmarkReaderPool(b, randBytes(b, TestFileSize))
}

// BenchmarkReaderConstant tests decoding of maximally compressible data.
func BenchmarkReaderConstant(b *testing.B) {
	encodeAndBenchmarkReader(b, make([]byte, TestFileSize))
}
func BenchmarkReaderConstantNoCopy(b *testing.B) {
	encodeAndBenchmarkReaderNoCopy(b, make([]byte, TestFileSize))
}
func BenchmarkReaderConstantNoReset(b *testing.B) {
	encodeAndBenchmarkReaderNoReset(b, make([]byte, TestFileSize))
}
func BenchmarkReaderConstantPool(b *testing.B) {
	encodeAndBenchmarkReaderPool(b, make([]byte, TestFileSize))
}

// encodeAndBenchmarkReader is a helper that benchmarks the package
// reader's performance given p encoded as a snappy framed stream.
//
// encodeAndBenchmarkReader benchmarks decoding of streams containing
// (multiple) short frames.
func encodeAndBenchmarkReader(b *testing.B, p []byte) {
	enc, err := encodeStreamBytes(p, false)
	if err != nil {
		b.Fatalf("pre-benchmark compression: %v", err)
	}
	r := NewReader(nil)
	dec := func(rnew io.Reader) io.ReadCloser {
		r.Reset(rnew)
		return ioutil.NopCloser(r)
	}
	benchmarkDecode(b, dec, int64(len(p)), enc)
}

// encodeAndBenchmarkReaderNoCopy is a helper that benchmarks the package
// reader's performance given p encoded as a snappy framed stream.
// encodeAndBenchmarReaderNoCopy avoids use of the reader's io.WriterTo
// interface.
//
// encodeAndBenchmarkReaderNoCopy benchmarks decoding of streams that
// contain at most one short frame (at the end).
func encodeAndBenchmarkReaderNoCopy(b *testing.B, p []byte) {
	enc, err := encodeStreamBytes(p, true)
	if err != nil {
		b.Fatalf("pre-benchmark compression: %v", err)
	}
	r := NewReader(nil)
	rnocopy := ioutil.NopCloser(r)
	dec := func(rnew io.Reader) io.ReadCloser {
		r.Reset(rnew)
		return ioutil.NopCloser(rnocopy)
	}
	benchmarkDecode(b, dec, int64(len(p)), enc)
}

// encodeAndBenchmarkReaderNoReset is a helper that benchmarks the
// package reader's performance given p encoded as a snappy framed stream.
// encodeAndBenchmarReaderNoReset allocates a new reader in each iteration.
//
// encodeAndBenchmarkReaderNoReset benchmarks decoding of streams that
// contain at most one short frame (at the end).
func encodeAndBenchmarkReaderNoReset(b *testing.B, p []byte) {
	enc, err := encodeStreamBytes(p, true)
	if err != nil {
		b.Fatalf("pre-benchmark compression: %v", err)
	}
	dec := func(r io.Reader) io.ReadCloser {
		return ioutil.NopCloser(NewReader(r))
	}
	benchmarkDecode(b, dec, int64(len(p)), enc)
}

// encodeAndBenchmarkReaderPool is a helper that benchmarks the package
// reader's performance given p encoded as a snappy framed stream.
// encodeAndBenchmarkReaderPool uses a sync.Pool to avoid extra allocations.
//
// encodeAndBenchmarkReaderNoReset benchmarks decoding of streams that
// contain at most one short frame (at the end).
func encodeAndBenchmarkReaderPool(b *testing.B, p []byte) {
	enc, err := encodeStreamBytes(p, true)
	if err != nil {
		b.Fatalf("pre-benchmark compression: %v", err)
	}
	pool := &sync.Pool{
		New: func() interface{} {
			return NewReader(nil)
		},
	}
	dec := func(r io.Reader) io.ReadCloser {
		pr := pool.Get().(*Reader)
		pr.Reset(r)
		return &poolReader{pool, pr}
	}
	benchmarkDecode(b, dec, int64(len(p)), enc)
}

// benchmarkDecode runs a benchmark that repeatedly decoded snappy
// framed bytes enc.  The length of the decoded result in each iteration must
// equal size.
func benchmarkDecode(b *testing.B, dec func(io.Reader) io.ReadCloser, size int64, enc []byte) {
	b.SetBytes(int64(len(enc))) // BUG this is probably wrong
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := dec(bytes.NewReader(enc))
		n, err := io.Copy(ioutil.Discard, r)
		if err != nil {
			b.Fatalf(err.Error())
		}
		if n != size {
			b.Fatalf("read wrong amount %d != %d", n, size)
		}
	}
	b.StopTimer()
}

// encodeStreamBytes is like encodeStream but operates on a byte slice.
// encodeStreamBytes ensures that long streams are not maximally compressed if
// buffer is false.
func encodeStreamBytes(b []byte, buffer bool) ([]byte, error) {
	return encodeStream(dummyBytesReader(b), buffer)
}

// encodeStream encodes data read from r as a snappy framed stream and returns
// the result as a byte slice.  if buffer is true the bytes from r are buffered
// to improve the resulting slice's compression ratio.
func encodeStream(r io.Reader, buffer bool) ([]byte, error) {
	var buf bytes.Buffer
	if !buffer {
		w := NewWriter(&buf)
		_, err := io.Copy(w, r)
		if err != nil {
			return nil, err
		}
		err = w.Close()
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	w := NewWriter(&buf)
	_, err := io.Copy(w, r)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// randBytes reads size bytes from the computer's cryptographic random source.
// the resulting bytes have approximately maximal entropy and are effectively
// uncompressible with any algorithm.
func randBytes(b *testing.B, size int) []byte {
	randp := make([]byte, size)
	_, err := io.ReadFull(rand.Reader, randp)
	if err != nil {
		b.Fatal(err)
	}
	return randp
}

// writeCloserNoCopy is an io.WriteCloser that simply wraps another
// io.WriteCloser.  This is useful for masking implementations for interfaces
// like ReaderFrom which may be opted into use inside functions like io.Copy.
type writeCloserNoCopy struct {
	io.WriteCloser
}

// nopWriteCloser is an io.WriteCloser that has a noop Close method.  This type
// has the effect of masking the underlying writer's Close implementation if it
// has one, or satisfying interface implementations for writers that do not
// need to be closing.
type nopWriteCloser struct {
	io.Writer
}

func (w *nopWriteCloser) Close() error {
	return nil
}

type poolWriter struct {
	p *sync.Pool
	*Writer
}

func (r *poolWriter) Close() error {
	err := r.Writer.Close()
	r.Writer.Reset(nil)
	r.p.Put(r.Writer)
	r.Writer = nil
	return err
}

type poolReader struct {
	p *sync.Pool
	*Reader
}

func (r *poolReader) Close() error {
	r.Reader.Reset(nil)
	r.p.Put(r.Reader)
	r.Reader = nil
	return nil
}
