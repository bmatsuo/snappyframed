## snappyframed

[![Build
Status](https://secure.travis-ci.org/bmatsuo/snappyframed.png?branch=master)](http://travis-ci.org/bmatsuo/snappyframed)
[![GoDoc](https://godoc.org/github.com/bmatsuo/snappyframed?status.svg)](https://godoc.org/github.com/bmatsuo/snappyframed)

This is a fork of the
[go-snappystream](https://github.com/mreiferson/ga-snappystream) package.  It
has a cleaner interface and extra optimization.

This repository uses semantic versioning.  If you want to protect yourself
against backwards incompatible changes (of which no further are anticipated),
you should the [gopkg.in](http://gopkg.in/bmatsuo/snappyframed.v1) import path,
"gopkg.in/bmatsuo/snappyframed.v1".

This package wraps [snappy][1] and supplies a buffered `Reader` and `Writer`
for the snappy [framed stream format][2].

[1]: https://github.com/golang/snappy
[2]: https://github.com/google/snappy/blob/master/framing_format.txt
