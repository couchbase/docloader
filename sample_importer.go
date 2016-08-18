/* Copyright (C) Couchbase, Inc 2016 - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */

package docloader

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io/ioutil"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/clog"
	"github.com/couchbase/gocb"
)

type jsonSampleImporter struct {
	host     string
	username string
	password string
	path     string
	rest     *RestClient
	zipfile  *zip.ReadCloser
}

func CreateJsonSampleImporter(host, username, password, path string) (*jsonSampleImporter, error) {
	r, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}

	return &jsonSampleImporter{
		host:    host,
		path:    path,
		rest:    CreateRestClient(host, username, password),
		zipfile: r,
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
	ddocs := make([]DDoc, 0)

	succeeded := true
	for _, f := range js.zipfile.File {
		doc := strings.Split(f.Name, "/design_docs/")
		if len(doc) == 2 && len(doc[1]) > 0 {
			if doc[1] == "indexes.json" {
				continue
			}

			clog.Log("Reading view definitions from %s", f.Name)

			data, err := readFile(*f)
			if err != nil {
				clog.Error(err)
				succeeded = false
				continue
			}

			type overlay struct {
				Id       string      `json:"_id"`
				Language string      `json:"language"`
				Views    interface{} `json:"views"`
				Spatial  interface{} `json:"spatial"`
			}

			var dd overlay
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.UseNumber()
			if err = decoder.Decode(&dd); err != nil {
				clog.Error(err)
				succeeded = false
				continue
			}

			if dd.Spatial == nil {
				ddocs = append(ddocs, DDoc{dd.Id, "", dd.Views})
			} else {
				code := make(map[string]interface{})
				code["id"] = dd.Id
				code["language"] = dd.Language
				code["spatial"] = dd.Spatial
				ddocs = append(ddocs, DDoc{dd.Id, "", code})
			}
		}
	}

	err := js.rest.PutViews(bucket, ddocs)
	if err != nil {
		clog.Error(err)
		succeeded = false
	}

	return succeeded
}

func (js *jsonSampleImporter) Queries(bucket string) bool {
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
	for _, f := range js.zipfile.File {
		doc := strings.Split(f.Name, "/design_docs/")
		if len(doc) == 2 && len(doc[1]) > 0 {
			if doc[1] != "indexes.json" {
				continue
			}

			clog.Log("Reading index definitions from %s", f.Name)
			data, err := readFile(*f)
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
				query := gocb.NewN1qlQuery(queryDef.Statement)
				query.Consistency(gocb.NotBounded)
				_, err := b.ExecuteN1qlQuery(query, nil)
				if err != nil {
					clog.Error(err)
					succeeded = false
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
	sent := uint64(0)
	inserted := uint64(0)
	complete := uint64(0)
	succeeded := true
	sendQ := make(chan *Pair, 2000)
	killQ := make(chan bool, threads)

	clog.Log("Loading data into the %s bucket", bucket)

	go func() {
		for _, f := range js.zipfile.File {
			doc := strings.Split(f.Name, "/docs/")
			if len(doc) == 2 && len(doc[1]) > 0 {
				key := doc[1]
				value, err := readFile(*f)
				if err != nil {
					clog.Error(err)
					succeeded = false
					continue
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
				atomic.LoadUint64(&read) == atomic.LoadUint64(&sent) {
				for i := 0; i < threads; i++ {
					killQ <- true
				}
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
					_, err = b.Upsert(pair.Key, pair.Value, 0)
					if err != nil {
						if err == gocb.ErrTmpFail {
							sendQ <- pair
							time.Sleep(250 * time.Millisecond)
							continue
						} else {
							clog.Error(err)
							succeeded = false
						}
					}
					atomic.AddUint64(&inserted, 1)
					atomic.AddUint64(&sent, 1)
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
	if err := js.zipfile.Close(); err != nil {
		clog.Error(err)
	}
}

func readFile(f zip.File) ([]byte, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	return data, err
}
