package httpclient

import (
  "bytes"
	"crypto/tls"
  "encoding/json"
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

func (ins *Instance) Execute(method string, url string, body io.Reader, auth *Auth, t interface{}) error {
	request, err := http.NewRequest(method, url, body)
	if err != nil {
		return err
	}
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
	return readTo(resp.Body, t)
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

