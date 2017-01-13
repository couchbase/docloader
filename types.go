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
	"encoding/json"
	"net/url"
	"strconv"
)

type FullTextIndex struct {
	Name         string      `json:"name"`
	Param        interface{} `json:"params"`
	PlanParams   interface{} `json:"planParams"`
	SourceName   string      `json:"sourceName"`
	SourceParams interface{} `json:"sourceParams"`
	SourceType   string      `json:"sourceType"`
	Type         string      `json:"type"`
}

type N1QLQuery struct {
	Statement string `json:"statement"`
	Args      string `json:"args"`
}

type BucketSettings struct {
	Name           string `json:"name"`
	BucketType     string `json:"bucketType"`
	Password       string `json:"saslPassword"`
	ProxyPort      int    `json:"proxyPort"`
	EvictionPolicy string `json:"evictionPolicy"`
	RAMQuota       int    `json:"ramQuota"`
	FlushEnabled   bool   `json:"flushEnabled"`
}

func UnmarshalBucketSettingsFromRest(msg json.RawMessage) (*BucketSettings, error) {
	var bs BucketSettings
	if err := json.Unmarshal(msg, &bs); err != nil {
		return nil, err
	}

	return &bs, nil
}

func (b *BucketSettings) FormEncoded() string {
	data := url.Values{}
	data.Add("name", b.Name)
	data.Add("bucketType", b.BucketType)
	if b.ProxyPort == 0 {
		data.Add("saslPassword", b.Password)
		data.Add("authType", "sasl")
	} else {
		data.Add("proxyPort", strconv.Itoa(b.ProxyPort))
		data.Add("authType", "none")
	}
	data.Add("evictionPolicy", b.EvictionPolicy)
	data.Add("ramQuotaMB", strconv.Itoa(b.RAMQuota))
	if b.FlushEnabled {
		data.Add("flushEnabled", "1")
	} else {
		data.Add("flushEnabled", "0")
	}

	return data.Encode()
}
