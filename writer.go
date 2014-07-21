package snappystream

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"code.google.com/p/snappy-go/snappy"
)

// includes block header
var streamID = []byte{0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59}

type writer struct {
	writer io.Writer

	hdr []byte
	dst []byte

	sentStreamID bool
}

// NewWriter returns an io.Writer interface to the snappy framed stream format.
//
// It transparently handles sending the stream identifier, calculating
// checksums, and compressing/framing blocks.
//
// Internally, a buffer is maintained to hold a compressed
// block.  It will automatically re-size up the the largest
// block size, 65536.
//
// For each Write, the returned length will only ever be len(p) or 0, regardless
// of the length of *compressed* bytes written to the wrapped io.Writer.
//
// If the returned length is 0 then error will be non-nil.
//
// If len(p) exceeds 65536, the slice will be automatically chunked into smaller blocks.
func NewWriter(w io.Writer) io.Writer {
	return &writer{
		writer: w,

		hdr: make([]byte, 8),
		dst: make([]byte, 4096),
	}
}

func (w *writer) Write(p []byte) (int, error) {
	total := 0
	sz := MaxBlockSize
	var n int
	for i := 0; i < len(p); i += n {
		if i+sz > len(p) {
			sz = len(p) - i
		}

		var err error
		n, err = w.write(p[i : i+sz])
		if err != nil {
			return 0, err
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

	if compressed {
		w.hdr[0] = 0x00 // compressed frame ID
	} else {
		w.hdr[0] = 0x01 // uncomprsessed frame ID
	}

	// 3 byte little endian length of encoded content
	length := uint32(len(block)) + 4 // +4 for checksum
	w.hdr[1] = byte(length)
	w.hdr[2] = byte(length >> 8)
	w.hdr[3] = byte(length >> 16)

	// 4 byte little endian CRC32 checksum of decoded content
	checksum := maskChecksum(crc32.Checksum(p[:n], crcTable))
	w.hdr[4] = byte(checksum)
	w.hdr[5] = byte(checksum >> 8)
	w.hdr[6] = byte(checksum >> 16)
	w.hdr[7] = byte(checksum >> 24)

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

func maskChecksum(c uint32) uint32 {
	return ((c >> 15) | (c << 17)) + 0xa282ead8
}
