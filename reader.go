package snappyframed

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"

	"code.google.com/p/snappy-go/snappy"
)

// errMssingStreamID is returned from a reader when the source stream does not
// begin with a stream identifier block (4.1 Stream identifier).  Its occurance
// signifies that the source byte stream is not snappy framed.
var errMissingStreamID = fmt.Errorf("missing stream identifier")

// Reader is an io.Reader that can reads data decompressed from a compressed
// snappy framed stream read with an underlying io.Reader.
type Reader struct {
	reader io.Reader

	err error

	seenStreamID bool

	buf bytes.Buffer
	hdr []byte
	src []byte
	dst []byte
}

// NewReader returns an new Reader. Reads from the Reader retreive data
// decompressed from a snappy framed stream read from sz.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		reader: r,

		// Internally, three buffers are maintained.  The first two are for reading
		// off the wrapped io.Reader and for holding the decompressed block (both are grown
		// automatically and re-used and will never exceed the largest block size, 65536). The
		// last buffer contains the *unread* decompressed bytes (and can grow indefinitely).
		hdr: make([]byte, 4),
		src: make([]byte, 4096),
		dst: make([]byte, 4096),
	}
}

// WriteTo implements the io.WriterTo interface used by io.Copy.  It writes
// decoded data from the underlying reader to w.  WriteTo returns the number of
// bytes written along with any error encountered.
func (sz *Reader) WriteTo(w io.Writer) (int64, error) {
	if sz.err != nil {
		return 0, sz.err
	}

	n, err := sz.buf.WriteTo(w)
	if err != nil {
		// r.err doesn't need to be set because a write error occurred and the
		// stream hasn't been corrupted.
		return n, err
	}

	// pass a bufferFallbackWriter to nextFrame so that write errors may be
	// recovered from, allowing the unwritten stream to be read successfully.
	wfallback := &bufferFallbackWriter{
		w:   w,
		buf: &sz.buf,
	}
	for {
		var m int
		m, err = sz.nextFrame(wfallback)
		if wfallback.writerErr != nil && err == nil {
			// a partial write was made before an error occurred and not all m
			// bytes were writen to w.  but decoded bytes were successfully
			// buffered and reading can resume later.
			n += wfallback.n
			return n, wfallback.writerErr
		}
		n += int64(m)
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			sz.err = err
			return n, err
		}
	}
	panic("unreachable")
}

// bufferFallbackWriter writes to an underlying io.Writer until an error
// occurs.  If a error occurs in the underlying io.Writer the value is saved
// for later inspection while the bufferFallbackWriter silently starts
// buffering all data written to it. From the caller's perspective
// bufferFallbackWriter has the same Write behavior has a bytes.Buffer.
//
// bufferFallbackWriter is useful for the reader.WriteTo method because it
// allows internal decoding routines to avoid interruption (and subsequent
// stream corruption) due to writing errors.
type bufferFallbackWriter struct {
	w         io.Writer
	buf       *bytes.Buffer
	n         int64 // number of bytes successfully written to w
	writerErr error // any error that ocurred writing to w
}

// Write attempts to write b to the underlying io.Writer.  If the underlying
// writer fails or has failed previously unwritten bytes are buffered
// internally.  Write never returns an error but may panic with
// bytes.ErrTooLarge if the buffer grows too large.
func (w *bufferFallbackWriter) Write(b []byte) (int, error) {
	if w.writerErr != nil {
		return w.buf.Write(b)
	}
	n, err := w.w.Write(b)
	w.n += int64(n)
	if err != nil {
		// begin buffering input. bytes.Buffer does not return errors and so we
		// do not need complex error handling here.
		w.writerErr = err
		w.Write(b[n:])
		return len(b), nil
	}
	return n, nil
}

// Reset discards internal state and sets the underlying reader to r.  Reset
// does not alter the reader's verification of checksums.  After Reset returns
// the reader is equivalent to one returned by NewReader.  Reusing readers with
// Reset can significantly reduce allocation overhead in applications making
// heavy use of snappy framed format streams.
func (sz *Reader) Reset(r io.Reader) {
	sz.err = nil
	sz.reader = r
	sz.buf.Truncate(0)
}

func (sz *Reader) read(b []byte) (int, error) {
	n, err := sz.buf.Read(b)
	sz.err = err
	return n, err
}

// Read fills b with any decoded data remaining in the Reader's internal
// buffers. When buffers are empty the Reader attempts to decode a data chunk
// from the underlying to fill b with.
//
// Read returns an error if the first chunk encountered in the underlying
// reader is not a snappy-framed stream identifier.
func (sz *Reader) Read(b []byte) (int, error) {
	if sz.err != nil {
		return 0, sz.err
	}

	if sz.buf.Len() < len(b) {
		_, sz.err = sz.nextFrame(&sz.buf)
		if sz.err == io.EOF {
			// fill b with any remaining bytes in the buffer.
			return sz.read(b)
		}
		if sz.err != nil {
			return 0, sz.err
		}
	}

	return sz.read(b)
}

func (sz *Reader) nextFrame(w io.Writer) (int, error) {
	for {
		// read the 4-byte snappy frame header
		_, err := io.ReadFull(sz.reader, sz.hdr)
		if err != nil {
			return 0, err
		}

		// a stream identifier may appear anywhere and contains no information.
		// it must appear at the beginning of the stream.  when found, validate
		// it and continue to the next block.
		if sz.hdr[0] == blockStreamIdentifier {
			err := sz.readStreamID()
			if err != nil {
				return 0, err
			}
			sz.seenStreamID = true
			continue
		}
		if !sz.seenStreamID {
			return 0, errMissingStreamID
		}

		switch typ := sz.hdr[0]; {
		case typ == blockCompressed || typ == blockUncompressed:
			return sz.decodeBlock(w)
		case typ == blockPadding || (0x80 <= typ && typ <= 0xfd):
			// skip blocks whose data must not be inspected (4.4 Padding, and 4.6
			// Reserved skippable chunks).
			err := sz.discardBlock()
			if err != nil {
				return 0, err
			}
			continue
		default:
			// typ must be unskippable range 0x02-0x7f.  Read the block in full
			// and return an error (4.5 Reserved unskippable chunks).
			err = sz.discardBlock()
			if err != nil {
				return 0, err
			}
			return 0, fmt.Errorf("unrecognized unskippable frame %#x", sz.hdr[0])
		}
	}
	panic("unreachable")
}

// decodeDataBlock assumes sz.hdr[0] to be either blockCompressed or
// blockUncompressed.
func (sz *Reader) decodeBlock(w io.Writer) (int, error) {
	// read compressed block data and determine if uncompressed data is too
	// large.
	buf, err := sz.readBlock()
	if err != nil {
		return 0, err
	}
	declen := len(buf[4:])
	if sz.hdr[0] == blockCompressed {
		declen, err = snappy.DecodedLen(buf[4:])
		if err != nil {
			return 0, err
		}
	}
	if declen > maxBlockSize {
		return 0, fmt.Errorf("decoded block data too large %d > %d", declen, maxBlockSize)
	}

	// decode data and verify its integrity using the little-endian crc32
	// preceding encoded data
	crc32le, blockdata := buf[:4], buf[4:]
	if sz.hdr[0] == blockCompressed {
		sz.dst, err = snappy.Decode(sz.dst, blockdata)
		if err != nil {
			return 0, err
		}
		blockdata = sz.dst
	}
	checksum := unmaskChecksum(uint32(crc32le[0]) | uint32(crc32le[1])<<8 | uint32(crc32le[2])<<16 | uint32(crc32le[3])<<24)
	actualChecksum := crc32.Checksum(blockdata, crcTable)
	if checksum != actualChecksum {
		return 0, fmt.Errorf("checksum does not match %x != %x", checksum, actualChecksum)
	}
	return w.Write(blockdata)
}

func (sz *Reader) readStreamID() error {
	// the length of the block is fixed so don't decode it from the header.
	if !bytes.Equal(sz.hdr, streamID[:4]) {
		return fmt.Errorf("invalid stream identifier length")
	}

	// read the identifier block data "sNaPpY"
	block := sz.src[:6]
	_, err := noeof(io.ReadFull(sz.reader, block))
	if err != nil {
		return err
	}
	if !bytes.Equal(block, streamID[4:]) {
		return fmt.Errorf("invalid stream identifier block")
	}
	return nil
}

func (sz *Reader) discardBlock() error {
	length := uint64(decodeLength(sz.hdr[1:]))
	_, err := noeof64(io.CopyN(ioutil.Discard, sz.reader, int64(length)))
	return err
}

func (sz *Reader) readBlock() ([]byte, error) {
	// check bounds on encoded length (+4 for checksum)
	length := decodeLength(sz.hdr[1:])
	if length > (maxEncodedBlockSize + 4) {
		return nil, fmt.Errorf("encoded block data too large %d > %d", length, (maxEncodedBlockSize + 4))
	}

	if int(length) > len(sz.src) {
		sz.src = make([]byte, length)
	}

	buf := sz.src[:length]
	_, err := noeof(io.ReadFull(sz.reader, buf))
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// decodeLength decodes a 24-bit (3-byte) little-endian length from b.
func decodeLength(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
}

func unmaskChecksum(c uint32) uint32 {
	x := c - 0xa282ead8
	return ((x >> 17) | (x << 15))
}

// noeof is used after reads in situations where EOF signifies invalid
// formatting or corruption.
func noeof(n int, err error) (int, error) {
	if err == io.EOF {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

// noeof64 is used after long reads (e.g. io.Copy) in situations where io.EOF
// signifies invalid formatting or corruption.
func noeof64(n int64, err error) (int64, error) {
	if err == io.EOF {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}
