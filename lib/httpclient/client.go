package httpclient

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Instance struct {
	client http.Client
}

type Auth struct {
	Username string
	Password string
}

type HttpRequest interface {
	SetUrl(string) HttpRequest
	SetMethod(string) HttpRequest
	AddHeader(string, string) HttpRequest
	AddHeaders(map[string]string) HttpRequest
	SetBody(io.Reader) HttpRequest
	SetAuth(*Auth) HttpRequest
	SetResult(interface{}) HttpRequest

	Url() string
	Method() string
	Headers() map[string]string
	Body() io.Reader
	Auth() *Auth
	Result() interface{}
}

type _httpRequest struct {
	url     string
	method  *string
	headers map[string]string
	body    io.Reader
	result  interface{}
	auth    *Auth
}

func (hr *_httpRequest) SetUrl(url string) HttpRequest {
	hr.url = url
	return hr
}
func (hr *_httpRequest) SetMethod(name string) HttpRequest {
	hr.method = &name
	return hr
}

func (hr *_httpRequest) AddHeader(key string, value string) HttpRequest {
	hr.headers[key] = value
	return hr
}
func (hr *_httpRequest) AddHeaders(headers map[string]string) HttpRequest {
	for k, v := range headers {
		hr.headers[k] = v
	}
	return hr
}

func (hr *_httpRequest) SetBody(rd io.Reader) HttpRequest {
	hr.body = rd
	return hr
}

func (hr *_httpRequest) SetResult(t interface{}) HttpRequest {
	hr.result = t
	return hr
}
func (hr *_httpRequest) SetAuth(auth *Auth) HttpRequest {
	hr.auth = auth
	return hr
}

func (hr *_httpRequest) Url() string {
	return hr.url
}
func (hr *_httpRequest) Headers() map[string]string {
	return hr.headers
}
func (hr *_httpRequest) Method() string {
	if hr.method == nil {
		if hr.body != nil {
			return "POST"
		}
		return "GET"
	}
	return *hr.method
}

func (hr *_httpRequest) Body() io.Reader {
	return hr.body
}
func (hr *_httpRequest) Auth() *Auth {
	return hr.auth
}

func (hr *_httpRequest) Result() interface{} {
	return hr.result
}

func (ins *Instance) Execute(req HttpRequest) error {
	if req.Result() == nil {
		return fmt.Errorf("set expected result object")
	}

	request, err := http.NewRequest(req.Method(), req.Url(), req.Body())

	if err != nil {
		return err
	}
	for k, v := range req.Headers() {
		request.Header.Add(k, v)
	}
	auth := req.Auth()
	if auth != nil {
		request.SetBasicAuth((*auth).Username, (*auth).Password)
	}

	resp, err := ins.client.Do(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return err
	}
	return readTo(resp.Body, req.Result())
}

func (ins *Instance) NewRequest() HttpRequest {
	return &_httpRequest{
		headers: make(map[string]string),
	}
}

type HttpOptions struct {
	VerifySsl bool
}

func NewClientWithOptions(options HttpOptions) *Instance {
	var transport *http.Transport
	transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: !options.VerifySsl},
	}
	client := http.Client{
		Transport: transport,
	}
	return &Instance{
		client: client,
	}
}

func NewClient() *Instance {
	return NewClientWithOptions(HttpOptions{VerifySsl: true})
}

func readTo(src io.Reader, t interface{}) error {
	bb := bytes.Buffer{}
	_, err := bb.ReadFrom(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(bb.Bytes(), t)
}
