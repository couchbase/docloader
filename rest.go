/*   Copyright 2016 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package docloader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type RestClientError struct {
	method string
	url    string
	err    error
}

func (e RestClientError) Error() string {
	return fmt.Sprintf("Rest client error (%s %s): %s", e.method, e.url, e.err)
}

type ServiceNotAvailableError struct {
	service string
}

func (e ServiceNotAvailableError) Error() string {
	return fmt.Sprintf("Service `%s` is not available on target cluster", e.service)
}

type HttpError struct {
	code     int
	method   string
	resource string
	body     string
}

func (e HttpError) Error() string {
	switch e.code {
	case http.StatusBadRequest:
		return fmt.Sprintf("Bad request executing %s %s due to %s",
			e.method, e.resource, e.body)
	case http.StatusUnauthorized:
		return fmt.Sprintf("Authentication error executing \"%s %s\" "+
			"check username and password", e.method, e.resource)
	case http.StatusInternalServerError:
		return fmt.Sprintf("Internal server error while executing \"%s %s\" "+
			"check the server logs for more details", e.method, e.resource)
	default:
		return fmt.Sprintf("Received error %d while executing \"%s %s\"",
			e.code, e.method, e.resource)
	}
}

func (e HttpError) Code() int {
	return e.code
}

type BucketNotFoundError struct {
	name string
}

func (e BucketNotFoundError) Error() string {
	return fmt.Sprintf("Bucket %s doesn't exist", e.name)
}

type Node struct {
	Hostname string       `json:"hostname"`
	Services NodeServices `json:"services"`
}

type NodeServices struct {
	Capi           int `json:"capi"`
	Management     int `json:"mgmt"`
	FullText       int `json:"fts"`
	SecondaryIndex int `json:"indexHttp"`
	N1QL           int `json:"n1ql"`
}

type RestClient struct {
	client   http.Client
	secure   bool
	host     string
	username string
	password string
}

func CreateRestClient(host, username, password string) *RestClient {
	return &RestClient{
		client:   http.Client{},
		host:     host,
		username: username,
		password: password,
	}
}

func (r *RestClient) PutViews(bucket, ddocName string, ddoc []byte) error {
	method := "PUT"
	host, err := r.viewsHost()
	if err != nil {
		return err
	}

	url := host + "/couchBase/" + bucket + "/" + ddocName
	req, err := http.NewRequest(method, url, bytes.NewBuffer(ddoc))
	if err != nil {
		return RestClientError{method, url, err}
	}
	if r.username != "" || r.password != "" {
		req.SetBasicAuth(r.username, r.password)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.executeRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return BucketNotFoundError{bucket}
	} else if resp.StatusCode != http.StatusCreated {
		msg, _ := ioutil.ReadAll(resp.Body)
		return HttpError{resp.StatusCode, method, url, string(msg)}
	}

	return nil
}

func (r *RestClient) GetBucketPassword(name string) (string, error) {
	url := r.host + "/pools/default/buckets/" + name
	resp, err := r.executeGet(url)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	var bs BucketSettings
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	err = decoder.Decode(&bs)
	if err != nil {
		return "", &RestClientError{"GET", url, err}
	}

	return bs.Password, nil
}

func (r *RestClient) BucketExists(name string) (bool, error) {
	url := r.host + "/pools/default/buckets"
	resp, err := r.executeGet(url)
	if err != nil {
		return false, err
	}

	defer resp.Body.Close()

	var body []json.RawMessage
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	err = decoder.Decode(&body)
	if err != nil {
		return false, &RestClientError{"GET", url, err}
	}

	for _, data := range body {
		var bs BucketSettings
		if err := json.Unmarshal(data, &bs); err != nil {
			return false, err
		}

		if bs.Name == name {
			return true, nil
		}
	}

	return false, nil
}

func (r *RestClient) CreateBucket(settings *BucketSettings) error {
	method := "POST"
	url := "/pools/default/buckets"

	data := []byte(settings.FormEncoded())

	req, err := http.NewRequest(method, r.host+url, bytes.NewBuffer(data))
	if err != nil {
		return RestClientError{method, url, err}
	}
	if r.username != "" || r.password != "" {
		req.SetBasicAuth(r.username, r.password)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := r.executeRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusBadRequest {
		defer resp.Body.Close()
		contents, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			contents = []byte("<no body>")
		}
		return HttpError{resp.StatusCode, method, url, string(contents)}
	} else if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {

		return HttpError{resp.StatusCode, method, url, ""}
	}

	bucketReady := make(chan bool, 1)
	go func() {
		for !r.isBucketReady(settings.Name) {
			time.Sleep(1 * time.Second)
		}
		bucketReady <- true
	}()
	bucketReadyTimeout := 30 * time.Second
	bucketReadyTimer := time.NewTimer(bucketReadyTimeout)
	defer bucketReadyTimer.Stop()
	select {
	case <-bucketReady:
		return nil
	case <-bucketReadyTimer.C:
		return fmt.Errorf("timed out after %s waiting for bucket %s to be ready", bucketReadyTimeout, settings.Name)
	}
}

func (r *RestClient) isBucketReady(bucket string) bool {
	url := r.host + "/pools/default/buckets/" + bucket

	resp, err := r.executeGet(url)
	if err != nil {
		return false
	}

	defer resp.Body.Close()

	type overlay struct {
		Nodes []struct {
			Status string `json:"status"`
		} `json:"nodes"`
	}

	var data overlay
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	if err = decoder.Decode(&data); err != nil {
		return false
	}

	if resp.StatusCode != http.StatusOK {
		return false
	}

	if len(data.Nodes) == 0 {
		return false
	}

	for _, node := range data.Nodes {
		if node.Status != "healthy" {
			return false
		}
	}

	return true
}

func (r *RestClient) PostFullTextIndexes(defs []FullTextIndex) error {
	if len(defs) == 0 {
		return nil
	}

	method := "PUT"
	host, err := r.fullTextHost()
	if err != nil {
		return err
	}

	for _, def := range defs {
		url := fmt.Sprintf("%s/api/index/%s?prevIndexUUID=*", host, def.Name)
		data, err := json.Marshal(def)
		if err != nil {
			return &RestClientError{method, url, err}
		}

		req, err := http.NewRequest(method, url, bytes.NewBuffer(data))
		if err != nil {
			return RestClientError{method, url, err}
		}
		if r.username != "" || r.password != "" {
			req.SetBasicAuth(r.username, r.password)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := r.executeRequest(req)
		if err != nil {
			return err
		}

		if resp.StatusCode == http.StatusBadRequest {
			defer resp.Body.Close()
			contents, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				contents = []byte("<no body>")
			}
			return HttpError{resp.StatusCode, method, url, string(contents)}
		} else if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
			return HttpError{resp.StatusCode, method, url, ""}
		}
	}

	return nil
}

func (r *RestClient) GetClusterNodes() ([]Node, error) {
	uri := r.host + "/pools/default/nodeServices"
	resp, err := r.executeGet(uri)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	type overlay struct {
		NodesExt []Node `json:"nodesExt"`
	}

	var data overlay
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	err = decoder.Decode(&data)
	if err != nil {
		return nil, &RestClientError{"GET", uri, err}
	}
	parsed, err := url.Parse(r.host)
	if err != nil {
		return nil, &RestClientError{"GET", uri, err}
	}
	hostname, _, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		return nil, &RestClientError{"GET", uri, err}
	}
	//If the hostname is a raw IPv6 address rebuild it with brackets
	if strings.ContainsAny(hostname, ":") {
		hostname = "[" + hostname + "]"
	}

	for i := 0; i < len(data.NodesExt); i++ {
		if data.NodesExt[i].Hostname == "" {
			data.NodesExt[i].Hostname = hostname
		//If the hostname is a raw IPv6 address rebuild it with brackets
		} else if strings.ContainsAny(data.NodesExt[i].Hostname, ":") {
			data.NodesExt[i].Hostname = "[" + data.NodesExt[i].Hostname + "]"
		}
	}

	return data.NodesExt, nil
}

func (r *RestClient) fullTextHost() (string, error) {
	nodes, err := r.GetClusterNodes()
	if err != nil {
		return "", err
	}

	for _, node := range nodes {
		if node.Services.FullText != 0 {
			return fmt.Sprintf("http://%s:%d", node.Hostname, node.Services.FullText), nil
		}
	}

	return "", ServiceNotAvailableError{"full text"}
}

func (r *RestClient) viewsHost() (string, error) {
	nodes, err := r.GetClusterNodes()
	if err != nil {
		return "", err
	}

	for _, node := range nodes {
		if node.Services.Capi != 0 {
			return fmt.Sprintf("http://%s:%d", node.Hostname, node.Services.Management), nil
		}
	}

	return "", ServiceNotAvailableError{"views"}
}

func (r *RestClient) hasN1qlService() (bool, error) {
	uri := r.host + "/pools/default"
	resp, err := r.executeGet(uri)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	type overlay struct {
		Nodes []struct {
			Services []string `json:"services"`
		} `json:"nodes"`
	}

	var data overlay
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	err = decoder.Decode(&data)
	if err != nil {
		return false, &RestClientError{"GET", uri, err}
	}

	for _, node := range data.Nodes {
		for _, service := range node.Services {
			if service == "n1ql" {
				return true, nil
			}
		}
	}

	return false, nil
}

func (r *RestClient) executeGet(uri string) (*http.Response, error) {
	method := "GET"
	req, err := http.NewRequest(method, uri, nil)
	if err != nil {
		return nil, &RestClientError{method, uri, err}
	}
	if r.username != "" || r.password != "" {
		req.SetBasicAuth(r.username, r.password)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, &RestClientError{req.Method, req.URL.String(), err}
	}

	if resp.StatusCode == http.StatusBadRequest {
		contents, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			contents = []byte("<no body>")
		}
		resp.Body.Close()
		return nil, HttpError{resp.StatusCode, method, uri, string(contents)}
	} else if resp.StatusCode != http.StatusOK {
		return nil, HttpError{resp.StatusCode, method, uri, ""}
	}

	return resp, nil
}

func (r *RestClient) executeRequest(req *http.Request) (*http.Response, error) {
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, &RestClientError{req.Method, req.URL.String(), err}
	}

	return resp, nil
}
