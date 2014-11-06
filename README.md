## snappyframed

[![Build
Status](https://secure.travis-ci.org/bmatsuo/snappyframed.png?branch=master)](http://travis-ci.org/bmatsuo/snappyframed)
[![GoDoc](https://godoc.org/github.com/bmatsuo/snappyframed?status.svg)](https://godoc.org/github.com/bmatsuo/snappyframed)

This is a fork of the
[go-snappystream](https://github.com/mreiferson/ga-snappystream) package.  It
has a cleaner interface and extra optimization.

This package wraps [snappy-go][1] and supplies a `Reader` and `Writer` for the
snappy [framed stream format][2].

[1]: https://code.google.com/p/snappy-go/
[2]: https://snappy.googlecode.com/svn/trunk/framing_format.txt
