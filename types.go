/* Copyright (C) Couchbase, Inc 2016 - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
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
	Password       string `json:"-"`
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
