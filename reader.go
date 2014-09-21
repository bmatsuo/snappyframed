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
// decompressed from a snappy framed stream read from r.
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
func (r *Reader) WriteTo(w io.Writer) (int64, error) {
	n, err := r.buf.WriteTo(w)
	if err != nil {
		return n, err
	}
	for {
		var m int
		m, err = r.nextFrame(w)
		n += int64(m)
		if err != nil {
			break
		}
	}
	if err == io.EOF {
		err = nil
	}
	return n, err
}

// Reset discards internal state and sets the underlying reader to r.  Reset
// does not alter the reader's verification of checksums.  After Reset returns
// the reader is equivalent to one returned by NewReader.  Reusing readers with
// Reset can significantly reduce allocation overhead in applications making
// heavy use of snappy framed format streams.
func (r *Reader) Reset(rnew io.Reader) {
	r.err = nil
	r.reader = rnew
	r.buf.Truncate(0)
}

func (r *Reader) read(b []byte) (int, error) {
	n, err := r.buf.Read(b)
	r.err = err
	return n, err
}

// Read fills b with any decoded data remaining in the Reader's internal
// buffers. When buffers are empty the Reader attempts to decode a data chunk
// from the underlying to fill b with.
func (r *Reader) Read(b []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}

	if r.buf.Len() < len(b) {
		_, r.err = r.nextFrame(&r.buf)
		if r.err == io.EOF {
			// fill b with any remaining bytes in the buffer.
			return r.read(b)
		}
		if r.err != nil {
			return 0, r.err
		}
	}

	return r.read(b)
}

func (r *Reader) nextFrame(w io.Writer) (int, error) {
	for {
		// read the 4-byte snappy frame header
		_, err := io.ReadFull(r.reader, r.hdr)
		if err != nil {
			return 0, err
		}

		// a stream identifier may appear anywhere and contains no information.
		// it must appear at the beginning of the stream.  when found, validate
		// it and continue to the next block.
		if r.hdr[0] == blockStreamIdentifier {
			err := r.readStreamID()
			if err != nil {
				return 0, err
			}
			r.seenStreamID = true
			continue
		}
		if !r.seenStreamID {
			return 0, errMissingStreamID
		}

		switch typ := r.hdr[0]; {
		case typ == blockCompressed || typ == blockUncompressed:
			return r.decodeBlock(w)
		case typ == blockPadding || (0x80 <= typ && typ <= 0xfd):
			// skip blocks whose data must not be inspected (4.4 Padding, and 4.6
			// Reserved skippable chunks).
			err := r.discardBlock()
			if err != nil {
				return 0, err
			}
			continue
		default:
			// typ must be unskippable range 0x02-0x7f.  Read the block in full
			// and return an error (4.5 Reserved unskippable chunks).
			err = r.discardBlock()
			if err != nil {
				return 0, err
			}
			return 0, fmt.Errorf("unrecognized unskippable frame %#x", r.hdr[0])
		}
	}
	panic("unreachable")
}

// decodeDataBlock assumes r.hdr[0] to be either blockCompressed or
// blockUncompressed.
func (r *Reader) decodeBlock(w io.Writer) (int, error) {
	// read compressed block data and determine if uncompressed data is too
	// large.
	buf, err := r.readBlock()
	if err != nil {
		return 0, err
	}
	declen := len(buf[4:])
	if r.hdr[0] == blockCompressed {
		declen, err = snappy.DecodedLen(buf[4:])
		if err != nil {
			return 0, err
		}
	}
	if declen > MaxBlockSize {
		return 0, fmt.Errorf("decoded block data too large %d > %d", declen, MaxBlockSize)
	}

	// decode data and verify its integrity using the little-endian crc32
	// preceding encoded data
	crc32le, blockdata := buf[:4], buf[4:]
	if r.hdr[0] == blockCompressed {
		r.dst, err = snappy.Decode(r.dst, blockdata)
		if err != nil {
			return 0, err
		}
		blockdata = r.dst
	}
	checksum := unmaskChecksum(uint32(crc32le[0]) | uint32(crc32le[1])<<8 | uint32(crc32le[2])<<16 | uint32(crc32le[3])<<24)
	actualChecksum := crc32.Checksum(blockdata, crcTable)
	if checksum != actualChecksum {
		return 0, fmt.Errorf("checksum does not match %x != %x", checksum, actualChecksum)
	}
	return w.Write(blockdata)
}

func (r *Reader) readStreamID() error {
	// the length of the block is fixed so don't decode it from the header.
	if !bytes.Equal(r.hdr, streamID[:4]) {
		return fmt.Errorf("invalid stream identifier length")
	}

	// read the identifier block data "sNaPpY"
	block := r.src[:6]
	_, err := noeof(io.ReadFull(r.reader, block))
	if err != nil {
		return err
	}
	if !bytes.Equal(block, streamID[4:]) {
		return fmt.Errorf("invalid stream identifier block")
	}
	return nil
}

func (r *Reader) discardBlock() error {
	length := uint64(decodeLength(r.hdr[1:]))
	_, err := noeof64(io.CopyN(ioutil.Discard, r.reader, int64(length)))
	return err
}

func (r *Reader) readBlock() ([]byte, error) {
	// check bounds on encoded length (+4 for checksum)
	length := decodeLength(r.hdr[1:])
	if length > (maxEncodedBlockSize + 4) {
		return nil, fmt.Errorf("encoded block data too large %d > %d", length, (maxEncodedBlockSize + 4))
	}

	if int(length) > len(r.src) {
		r.src = make([]byte, length)
	}

	buf := r.src[:length]
	_, err := noeof(io.ReadFull(r.reader, buf))
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
