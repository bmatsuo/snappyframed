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
// compressed and written to w.  Before the first compressed chunked is written
// a snappy-framed stream identifier block is written to w.
//
// The caller is responsible for calling Flush or Close after all writes have
// completed to guarantee all data has been encoded and written to w.
func NewWriter(w io.Writer) *Writer {
	sz := newWriter(w)
	return &Writer{
		w:  sz,
		bw: bufio.NewWriterSize(sz, maxBlockSize),
	}
}

// ReadFrom implements the io.ReaderFrom interface used by io.Copy. It encodes
// data read from r as a snappy framed stream and writes the result to the
// underlying io.Writer.  ReadFrom returns the number number of bytes read,
// along with any error encountered (other than io.EOF).
func (sz *Writer) ReadFrom(r io.Reader) (int64, error) {
	if sz.err != nil {
		return 0, sz.err
	}

	var n int64
	n, sz.err = sz.bw.ReadFrom(r)
	return n, sz.err
}

// Reset discards internal state and sets the underlying writer to w.  After
// Reset returns the writer is equivalent to one returned by NewWriter(w).
// Reusing writers with Reset can significantly reduce allocation overhead in
// applications making heavy use of snappy framed format streams.
func (sz *Writer) Reset(w io.Writer) {
	sz.err = nil
	sz.w.Reset(w)
	sz.bw.Reset(sz.w)
}

// Write compresses the bytes of p and writes sequence of encoded chunks to the
// underlying io.Writer.  A chunked containing the compressed bytes of p may
// not be written to the underlying io.Writer by the time Write returns.
//
// Write returns 0 if and only if the returned error is non-nil.
func (sz *Writer) Write(p []byte) (int, error) {
	if sz.err != nil {
		return 0, sz.err
	}

	_, sz.err = sz.bw.Write(p)
	if sz.err != nil {
		return 0, sz.err
	}

	return len(p), nil
}

// Flush encodes any (decoded) source data buffered interanally in the Writer
// and writes a chunk containing the result to the underlying io.Writer.
func (sz *Writer) Flush() error {
	if sz.err == nil {
		sz.err = sz.bw.Flush()
	}

	return sz.err
}

// Close flushes the Writer and tears down internal data structures.  Close
// does not close the underlying io.Writer.
func (sz *Writer) Close() error {
	if sz.err != nil {
		return sz.err
	}

	sz.err = sz.bw.Flush()
	if sz.err != nil {
		return sz.err
	}

	sz.err = errClosed
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
// emit a block with length in bytes greater than maxBlockSize+4 nor one
// containing more than maxBlockSize bytes of (uncompressed) data.
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

// Reset discards internal state and sets the underlying writer to w.  After
// Reset returns the writer is equivalent to one returned by NewWriter(w).
// Reusing writers with Reset can significantly reduce allocation overhead in
// applications making heavy use of snappy framed format streams.
func (sz *writer) Reset(w io.Writer) {
	sz.err = nil
	sz.sentStreamID = false
	sz.writer = w
}

func (sz *writer) Write(p []byte) (int, error) {
	if sz.err != nil {
		return 0, sz.err
	}

	total := 0
	size := maxBlockSize
	var n int
	for i := 0; i < len(p); i += n {
		if i+size > len(p) {
			size = len(p) - i
		}

		n, sz.err = sz.write(p[i : i+size])
		if sz.err != nil {
			return 0, sz.err
		}
		total += n
	}
	return total, nil
}

// write attempts to encode p as a block and write it to the underlying writer.
// The returned int may not equal p's length if compression below
// maxBlockSize-4 could not be achieved.
func (sz *writer) write(p []byte) (int, error) {
	var err error

	if len(p) > maxBlockSize {
		return 0, errors.New(fmt.Sprintf("block too large %d > %d", len(p), maxBlockSize))
	}

	sz.dst = sz.dst[:cap(sz.dst)] // Encode does dumb resize w/o context. reslice avoids alloc.
	sz.dst, err = snappy.Encode(sz.dst, p)
	if err != nil {
		return 0, err
	}
	block := sz.dst
	n := len(p)
	compressed := true

	// check for data which is better left uncompressed.  this is determined if
	// the encoded content is longer than the source.
	if len(sz.dst) >= len(p) {
		compressed = false
		block = p[:n]
	}

	if !sz.sentStreamID {
		_, err := sz.writer.Write(streamID)
		if err != nil {
			return 0, err
		}
		sz.sentStreamID = true
	}

	// set the block type
	if compressed {
		writeHeader(sz.hdr, blockCompressed, block, p[:n])
	} else {
		writeHeader(sz.hdr, blockUncompressed, block, p[:n])
	}

	_, err = sz.writer.Write(sz.hdr)
	if err != nil {
		return 0, err
	}

	_, err = sz.writer.Write(block)
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
