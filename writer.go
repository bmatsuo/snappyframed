package snappyframed

import (
	"bufio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"code.google.com/p/snappy-go/snappy"
)

var errClosed = fmt.Errorf("closed")

// Writer is an io.WriteCloser, Data written to a Writer is compressed and
// flushed to an underlying io.Writer.
type Writer struct {
	err error
	w   *writer
	bw  *bufio.Writer
}

// NewWriter returns a new Writer.  Data written to the returned Writer is
// compressed and written to w.
//
// The caller is responsible for calling Flush or Close after all writes have
// completed to guarantee all data has been encoded and written to w.
func NewWriter(w io.Writer) *Writer {
	sz := newWriter(w)
	return &Writer{
		w:  sz,
		bw: bufio.NewWriterSize(sz, MaxBlockSize),
	}
}

// ReadFrom implements the io.ReaderFrom interface used by io.Copy. It encodes
// data read from r as a snappy framed stream and writes the result to the
// underlying io.Writer.  ReadFrom returns the number number of bytes read,
// along with any error encountered (other than io.EOF).
func (w *Writer) ReadFrom(r io.Reader) (int64, error) {
	if w.err != nil {
		return 0, w.err
	}

	var n int64
	n, w.err = w.bw.ReadFrom(r)
	return n, w.err
}

// Write compresses the bytes of p and writes sequence of encoded chunks to the
// underlying io.Writer.  Because decompressed data is buffered internally
// before encoding calls to Write may not always result in data being written
// to the underlying io.Writer.
//
// Write returns 0 if and only if the returned error is non-nil.
func (w *Writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}

	_, w.err = w.bw.Write(p)
	if w.err != nil {
		return 0, w.err
	}

	return len(p), nil
}

// Flush encodes any (decoded) source data buffered interanally in the Writer
// and writes a chunk containing the result to the underlying io.Writer.
func (w *Writer) Flush() error {
	if w.err == nil {
		w.err = w.bw.Flush()
	}

	return w.err
}

// Close flushes the Writer and tears down internal data structures.  Close
// does not close the underlying io.Writer.
func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}

	w.err = w.bw.Flush()
	w.w = nil
	w.bw = nil

	if w.err != nil {
		return w.err
	}

	w.err = errClosed
	return nil
}

type writer struct {
	writer io.Writer
	err    error

	hdr []byte
	dst []byte

	sentStreamID bool
}

// newWriter returns an io.Writer that writes its input to an underlying
// io.Writer encoded as a snappy framed stream.  A stream identifier block is
// written to w preceding the first data block.  The returned writer will never
// emit a block with length in bytes greater than MaxBlockSize+4 nor one
// containing more than MaxBlockSize bytes of (uncompressed) data.
//
// For each Write, the returned length will only ever be len(p) or 0,
// regardless of the length of *compressed* bytes written to the wrapped
// io.Writer.  If the returned length is 0 then error will be non-nil.  If
// len(p) exceeds 65536, the slice will be automatically chunked into smaller
// blocks which are all emitted before the call returns.
func newWriter(w io.Writer) *writer {
	return &writer{
		writer: w,

		hdr: make([]byte, 8),
		dst: make([]byte, 4096),
	}
}

func (w *writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}

	total := 0
	sz := MaxBlockSize
	var n int
	for i := 0; i < len(p); i += n {
		if i+sz > len(p) {
			sz = len(p) - i
		}

		n, w.err = w.write(p[i : i+sz])
		if w.err != nil {
			return 0, w.err
		}
		total += n
	}
	return total, nil
}

// write attempts to encode p as a block and write it to the underlying writer.
// The returned int may not equal p's length if compression below
// MaxBlockSize-4 could not be achieved.
func (w *writer) write(p []byte) (int, error) {
	var err error

	if len(p) > MaxBlockSize {
		return 0, errors.New(fmt.Sprintf("block too large %d > %d", len(p), MaxBlockSize))
	}

	w.dst = w.dst[:cap(w.dst)] // Encode does dumb resize w/o context. reslice avoids alloc.
	w.dst, err = snappy.Encode(w.dst, p)
	if err != nil {
		return 0, err
	}
	block := w.dst
	n := len(p)
	compressed := true

	// check for data which is better left uncompressed.  this is determined if
	// the encoded content is longer than the source.
	if len(w.dst) >= len(p) {
		compressed = false
		block = p[:n]
	}

	if !w.sentStreamID {
		_, err := w.writer.Write(streamID)
		if err != nil {
			return 0, err
		}
		w.sentStreamID = true
	}

	// set the block type
	if compressed {
		writeHeader(w.hdr, blockCompressed, block, p[:n])
	} else {
		writeHeader(w.hdr, blockUncompressed, block, p[:n])
	}

	_, err = w.writer.Write(w.hdr)
	if err != nil {
		return 0, err
	}

	_, err = w.writer.Write(block)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// writeHeader panics if len(hdr) is less than 8.
func writeHeader(hdr []byte, btype byte, enc, dec []byte) {
	hdr[0] = btype

	// 3 byte little endian length of encoded content
	length := uint32(len(enc)) + 4 // +4 for checksum
	hdr[1] = byte(length)
	hdr[2] = byte(length >> 8)
	hdr[3] = byte(length >> 16)

	// 4 byte little endian CRC32 checksum of decoded content
	checksum := maskChecksum(crc32.Checksum(dec, crcTable))
	hdr[4] = byte(checksum)
	hdr[5] = byte(checksum >> 8)
	hdr[6] = byte(checksum >> 16)
	hdr[7] = byte(checksum >> 24)
}
