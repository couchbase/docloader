// +build !windows

/* Copyright (C) Couchbase, Inc 2016 - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */

package man

import (
	"path/filepath"
)

func CouchbaseInstallPath(exedir string) string {
	return filepath.Join(filepath.Dir(exedir), "share", "man")
}

func StandaloneInstallPath(exedir string) string {
	return filepath.Join(filepath.Dir(filepath.Dir(exedir)), "man")
}

func DocloaderManual() string {
	return filepath.Join("man1", "cbdocloader.1")
}
