package snappyframed_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"

	snappyframed "."
)

// readerPool and writerPool are the simplest pool implementations for decoding
// and encoding snappyframed streams.  A real application may wish to wrap
// pools in a utility package that handles calling Get and Put on the pool
// transparently.
var readerPool = sync.Pool{New: func() interface{} { return snappyframed.NewReader(nil) }}
var writerPool = sync.Pool{New: func() interface{} { return snappyframed.NewWriter(nil) }}

// APIRequest is sent to the API in a JSON POST request.
type APIRequest struct {
	Name string
}

// APIResponse is sent to the API client in response to an APIRequest.
type APIResponse struct {
	Messages []string
}

// szAPIHandler reads a snappyframed stream encoding a JSON APIRequest and
// writes a JSON APIResponse encoded as a snappyframed stream.
func szAPIHandler(resp http.ResponseWriter, r *http.Request) {
	// decode the request entity as a snappy-framed stream.
	body := r.Body // it seems important not to reassign r.Body..
	defer body.Close()
	if r.Header.Get("Content-Type") == snappyframed.MediaType {
		// get a reader from the pool, replacing it when the handler has
		// completed.  then set the underlying reader r.Body and reset to nil
		// when the handler has completed.
		sz := readerPool.Get().(*snappyframed.Reader)
		defer readerPool.Put(sz)
		sz.Reset(body)
		defer sz.Reset(nil)
		body = ioutil.NopCloser(sz)
	}

	// decode the an APIRequest from the request entity.
	var req *APIRequest
	err := json.NewDecoder(body).Decode(&req)
	if err != nil {
		http.Error(resp, "invalid request", http.StatusBadRequest)
		return
	}

	// encode the response as a snappyframed stream.  the timing of statements
	// here is important and tricky.  A real application would probably want to
	// implement an http.ResponseWriter capable of doing HTTP content
	// negotiation and performing Reset automatically on creation and on Close.
	resp.Header().Set("Content-Type", snappyframed.MediaType)
	w := writerPool.Get().(*snappyframed.Writer)
	defer writerPool.Put(w)
	w.Reset(resp)
	defer w.Reset(nil)
	defer w.Close()

	json.NewEncoder(w).Encode(APIResponse{
		Messages: []string{
			fmt.Sprintf("hello %s", req.Name),
		},
	})
}

// This example shows how a sync.Pool might be used to speed up decoding and
// encoding of HTTP traffic.  Depending on your application such an approach
// may not provide any benefit.  Before making such a change it is important to
// instrument your system to measure performance gains.
func Example_pool() {
	server := httptest.NewServer(http.HandlerFunc(szAPIHandler))

	// encode a request body using the pool. in this case no defer statements
	// are used to clarify the order in which events happen.  some applications
	// may benefit from not using defer statements.
	req := APIRequest{"pooling example"}
	var reqbuf bytes.Buffer
	enc := writerPool.Get().(*snappyframed.Writer)
	enc.Reset(&reqbuf)
	json.NewEncoder(enc).Encode(req)
	enc.Close()
	enc.Reset(nil)
	writerPool.Put(enc)

	resp, err := http.Post(server.URL, snappyframed.MediaType, &reqbuf)
	if err != nil {
		panic(err)
	}

	// decode the response as a snappyframed stream.  don't bother unmarshaling
	// the response in this case and just write it to stdout.
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") == snappyframed.MediaType {
		sz := readerPool.Get().(*snappyframed.Reader)
		defer readerPool.Put(sz)
		sz.Reset(resp.Body)
		defer sz.Reset(nil)
		resp.Body = ioutil.NopCloser(sz)
	}
	_, err = io.Copy(os.Stdout, resp.Body)
	if err != nil {
		panic(err)
	}
	// Output: {"Messages":["hello pooling example"]}
}
