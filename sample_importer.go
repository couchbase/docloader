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
	succeeded := true
	for _, f := range js.sample.Files {
		if f.IsDir() {
			continue
		}

		docsDir := JoinPathWithSep(f.Seperator(), js.sample.Basepath, "design_docs")
		if strings.HasPrefix(f.Path(), docsDir) {
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

	c, _ := gocb.Connect(js.host)
	b, err := c.OpenBucket(bucket, "")
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

		docsDir := JoinPathWithSep(f.Seperator(), js.sample.Basepath, "design_docs")
		if strings.HasPrefix(f.Path(), docsDir) {
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
	c, _ := gocb.Connect(js.host)
	b, err := c.OpenBucket(bucket, "")
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

			docsDir := JoinPathWithSep(f.Seperator(), js.sample.Basepath, "docs")
			if strings.HasPrefix(f.Path(), docsDir) {
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
			b, err := c.OpenBucket(bucket, "")
			if err != nil {
				clog.Error(err)
				return
			}
			defer b.Close()

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
							if err == gocb.ErrTmpFail || err == gocb.ErrOutOfMemory {
								time.Sleep(250 * time.Millisecond)
								sentPair = false
							} else {
								clog.Error(err)
							}
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
	closer   io.Closer
	Basepath string
	Files    []SamplesFile
}

func OpenSamplesReader(path string) (*SamplesReader, error) {
	if strings.HasSuffix(path, ".zip") {
		closer, err := zip.OpenReader(path)
		if err != nil {
			return nil, err
		}

		filename := filepath.Base(path)
		rv := &SamplesReader{
			closer:   closer,
			Basepath: strings.TrimSuffix(filename, filepath.Ext(filename)),
			Files:    make([]SamplesFile, len(closer.File)),
		}

		for i, file := range closer.File {
			rv.Files[i] = &SamplesZipFile{
				file: file,
			}
		}

		return rv, nil
	} else {
		rv := &SamplesReader{
			closer:   DirCloser{},
			Basepath: path,
			Files:    make([]SamplesFile, 0),
		}
		err := filepath.Walk(path, rv.addFile)
		return rv, err
	}
}

func (r *SamplesReader) addFile(path string, f os.FileInfo, err error) error {
	if err == nil {
		r.Files = append(r.Files, &SamplesDirFile{path})
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
