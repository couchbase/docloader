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
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/clog"
	"github.com/couchbase/gocb"
)

const maxConsecutiveTimeouts int = 10

type jsonSampleImporter struct {
	host   string
	path   string
	rest   *RestClient
	sample *SamplesReader
}

func CreateJsonSampleImporter(host, username, password, path string) (*jsonSampleImporter, error) {
	sample, err := OpenSamplesReader(path)
	if err != nil {
		return nil, err
	}

	return &jsonSampleImporter{
		host:   host,
		path:   path,
		rest:   CreateRestClient(host, username, password),
		sample: sample,
	}, nil
}

func (js *jsonSampleImporter) CreateBucket(bucket string, memQuota int) bool {
	exists, err := js.rest.BucketExists(bucket)
	if err != nil {
		clog.Error(err)
		return false
	}

	if exists {
		clog.Log("Bucket `%s` already exists, skipping creation", bucket)
		return true
	}

	clog.Log("Creating `%s` bucket", bucket)
	settings := &BucketSettings{
		Name:           bucket,
		BucketType:     "membase",
		Password:       "",
		ProxyPort:      0,
		EvictionPolicy: "valueOnly",
		RAMQuota:       memQuota,
		FlushEnabled:   false,
	}

	err = js.rest.CreateBucket(settings)
	if err != nil {
		clog.Error(err)
		return false
	}

	clog.Log("Bucket `%s` created", bucket)
	return true
}

func (js *jsonSampleImporter) Views(bucket string) bool {
	if !js.sample.HasDDocsPath {
		return true
	}

	succeeded := true
	for _, f := range js.sample.Files {
		if f.IsDir() {
			continue
		}

		if strings.HasPrefix(f.Path(), js.sample.DDocsPath) {
			if filepath.Base(f.Path()) == "indexes.json" {
				continue
			}

			clog.Log("Reading view definitions from %s", f.Path())

			data, err := f.ReadFile()
			if err != nil {
				clog.Error(err)
				succeeded = false
				continue
			}

			type overlay struct {
				Id string `json:"_id"`
			}

			var dd overlay
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.UseNumber()
			if err = decoder.Decode(&dd); err != nil {
				clog.Error(err)
				succeeded = false
				continue
			}

			err = js.rest.PutViews(bucket, dd.Id, data)
			if err != nil {
				clog.Error(err)
				succeeded = false
			}
		}
	}

	return succeeded
}

func (js *jsonSampleImporter) Queries(bucket string) bool {
	waitForN1QL, err := js.rest.hasN1qlService()
	if err != nil {
		clog.Error(err)
		return false
	}

	password, err := js.rest.GetBucketPassword(bucket)
	if err != nil {
		clog.Error(err)
		return false
	}

	c, _ := gocb.Connect(js.host)
	b, err := c.OpenBucket(bucket, password)
	if err != nil {
		clog.Error(err)
		return false
	}
	defer b.Close()

	type overlay struct {
		Statements []N1QLQuery `json:"statements"`
	}

	succeeded := true
	for _, f := range js.sample.Files {
		if f.IsDir() {
			continue
		}

		if strings.HasPrefix(f.Path(), js.sample.DDocsPath) {
			if filepath.Base(f.Path()) != "indexes.json" {
				continue
			}

			clog.Log("Reading index definitions from %s", f.Path())
			data, err := f.ReadFile()
			if err != nil {
				clog.Error(err)
				succeeded = false
			}

			var stmts overlay
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.UseNumber()
			if err = decoder.Decode(&stmts); err != nil {
				clog.Error(err)
				succeeded = false
				continue
			}

			// TODO: Need to account for arguments in N1QLQuery structure
			for _, queryDef := range stmts.Statements {
				sendQuery := true
				for sendQuery {
					sendQuery = false
					query := gocb.NewN1qlQuery(queryDef.Statement)
					query.Consistency(gocb.NotBounded)
					_, err := b.ExecuteN1qlQuery(query, nil)
					if err != nil {
						if waitForN1QL && err.Error() == "No available N1QL nodes." {
							clog.Log("N1QL Service not ready yet, retrying")
							time.Sleep(1 * time.Second)
							sendQuery = true
						} else {
							clog.Error(err)
							succeeded = false
						}
					}
				}
			}
		}
	}

	return succeeded
}

func (js *jsonSampleImporter) IterateDocs(bucket string, threads int) bool {
	password, err := js.rest.GetBucketPassword(bucket)
	if err != nil {
		clog.Error(err)
		return false
	}

	if !js.sample.HasDocsPath {
		return true
	}

	c, _ := gocb.Connect(js.host)
	b, err := c.OpenBucket(bucket, password)
	if err != nil {
		clog.Error(err)
		return false
	}

	defer b.Close()

	type Pair struct {
		Key   string
		Value []byte
	}

	read := uint64(0)
	inserted := uint64(0)
	complete := uint64(0)
	succeeded := true
	sendQ := make(chan *Pair, 2000)
	killQ := make(chan bool, threads)

	clog.Log("Loading data into the %s bucket", bucket)

	go func() {
		for _, f := range js.sample.Files {
			if f.IsDir() {
				continue
			}

			if strings.HasPrefix(f.Path(), js.sample.DocsPath) {
				key := filepath.Base(f.Path())
				value, err := f.ReadFile()
				if err != nil {
					clog.Error(err)
					succeeded = false
					continue
				}

				if filepath.Ext(f.Path()) == ".json" {
					key = key[:len(key)-5]
				}

				atomic.AddUint64(&read, 1)
				sendQ <- &Pair{key, value}
			}
		}
		atomic.StoreUint64(&complete, 1)
	}()

	go func() {
		for true {
			if atomic.LoadUint64(&complete) == 1 &&
				atomic.LoadUint64(&read) == atomic.LoadUint64(&inserted) {
				for i := 0; i < threads; i++ {
					killQ <- true
				}
				return
			} else {
				time.Sleep(250 * time.Millisecond)
			}
		}
	}()

	wg := &sync.WaitGroup{}
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, _ := gocb.Connect(js.host)
			b, err := c.OpenBucket(bucket, password)
			if err != nil {
				clog.Error(err)
				return
			}
			defer b.Close()

			timeoutCount := 0
			for true {
				select {
				case pair := <-sendQ:
					sentPair := false
					for !sentPair {
						sentPair = true

						var js map[string]interface{}
						decoder := json.NewDecoder(bytes.NewReader(pair.Value))
						decoder.UseNumber()
						if err := decoder.Decode(&js); err != nil {
							clog.Error(fmt.Errorf("File %s.json does not contain valid JSON", pair.Key))
						}

						_, err = b.Upsert(pair.Key, js, 0)
						if err != nil {
							if err == gocb.ErrNetwork {
								clog.Error(err)
								return
							} else if err == gocb.ErrTmpFail || err == gocb.ErrOutOfMemory {
								time.Sleep(250 * time.Millisecond)
								sentPair = false
								timeoutCount = 0
							} else {
								if timeoutCount >= maxConsecutiveTimeouts {
									clog.Log("%d consecutive timeouts occurred, client exiting",
										maxConsecutiveTimeouts)
									return
								}
								timeoutCount++
								clog.Error(err)
							}
						} else {
							timeoutCount = 0
						}
					}
					atomic.AddUint64(&inserted, 1)
				case <-killQ:
					return
				}
			}
		}()
	}

	wg.Wait()

	clog.Log("Loaded %d items into the %s bucket", atomic.LoadUint64(&inserted), bucket)
	return succeeded
}

func (js *jsonSampleImporter) Close() {
	if err := js.sample.Close(); err != nil {
		clog.Error(err)
	}
}

type SamplesReader struct {
	closer       io.Closer
	HasDocsPath  bool
	HasDDocsPath bool
	DocsPath     string
	DDocsPath    string
	Files        []SamplesFile
}

func OpenSamplesReader(path string) (*SamplesReader, error) {
	if strings.HasSuffix(path, ".zip") {
		closer, err := zip.OpenReader(path)
		if err != nil {
			return nil, err
		}

		rv := &SamplesReader{
			closer:       closer,
			HasDocsPath:  false,
			HasDDocsPath: false,
			DocsPath:     "",
			DDocsPath:    "",
			Files:        make([]SamplesFile, len(closer.File)),
		}

		for i, file := range closer.File {
			if file.FileInfo().IsDir() {
				dname := file.Name
				if strings.HasSuffix(dname, "/") {
					dname = dname[0 : len(dname)-1]
				}
				_, dir := filepath.Split(dname)

				if dir == "docs" {
					if rv.HasDocsPath {
						return nil, fmt.Errorf("Samples zip file may only contain one docs directory")
					}
					rv.HasDocsPath = true
					rv.DocsPath = file.Name
				}

				if dir == "design_docs" {
					if rv.HasDDocsPath {
						return nil, fmt.Errorf("Samples zip file may only contain one design_docs directory")
					}
					rv.HasDDocsPath = true
					rv.DDocsPath = file.Name
				}
			}
			rv.Files[i] = &SamplesZipFile{
				file: file,
			}
		}

		if !rv.HasDocsPath && !rv.HasDDocsPath {
			rv.HasDocsPath = true
			rv.DocsPath = "/"
		}

		return rv, nil
	} else {
		rv := &SamplesReader{
			closer:       DirCloser{},
			HasDocsPath:  false,
			HasDDocsPath: false,
			DocsPath:     "",
			DDocsPath:    "",
			Files:        make([]SamplesFile, 0),
		}
		err := filepath.Walk(path, rv.addFile)
		if err != nil {
			return nil, err
		}

		if !rv.HasDocsPath && !rv.HasDDocsPath {
			rv.HasDocsPath = true
			rv.DocsPath = path
		}

		return rv, nil
	}
}

func (r *SamplesReader) addFile(path string, f os.FileInfo, err error) error {
	if err == nil {
		r.Files = append(r.Files, &SamplesDirFile{path})

		if f.IsDir() {
			dname := path
			if strings.HasSuffix(dname, string(os.PathSeparator)) {
				dname = dname[0 : len(dname)-1]
			}
			_, dir := filepath.Split(dname)

			if dir == "docs" {
				if r.HasDocsPath {
					return fmt.Errorf("Samples folder may only contain one docs directory")
				}
				r.HasDocsPath = true
				r.DocsPath = path
			}

			if dir == "design_docs" {
				if r.HasDDocsPath {
					return fmt.Errorf("Samples folder may only contain one design_docs directory")
				}
				r.HasDDocsPath = true
				r.DDocsPath = path
			}
		}

	} else {
		clog.Error(err)
	}
	return nil
}

func (r *SamplesReader) Close() error {
	return r.closer.Close()
}

type SamplesFile interface {
	Path() string
	Seperator() string
	ReadFile() ([]byte, error)
	IsDir() bool
}

type SamplesDirFile struct {
	path string
}

func (f *SamplesDirFile) Path() string {
	return f.path
}

func (f *SamplesDirFile) Seperator() string {
	return (string)(filepath.Separator)
}

func (f *SamplesDirFile) IsDir() bool {
	if src, err := os.Stat(f.path); err != nil {
		return false
	} else if src.IsDir() {
		return true
	}

	return false
}

func (f *SamplesDirFile) ReadFile() ([]byte, error) {
	return ioutil.ReadFile(f.path)
}

type SamplesZipFile struct {
	file *zip.File
}

func (f *SamplesZipFile) Path() string {
	return f.file.Name
}

func (f *SamplesZipFile) Seperator() string {
	return "/"
}

func (f *SamplesZipFile) IsDir() bool {
	return f.file.FileInfo().IsDir()
}

func (f *SamplesZipFile) ReadFile() ([]byte, error) {
	rc, err := f.file.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	return data, err
}

type DirCloser struct {
	io.Closer
}

func (d DirCloser) Close() error {
	return nil
}

func JoinPathWithSep(sep string, elem ...string) string {
	for i, e := range elem {
		if e != "" {
			return path.Clean(strings.Join(elem[i:], sep))
		}
	}

	return ""
}
