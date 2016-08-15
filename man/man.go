/* Copyright (C) Couchbase, Inc 2016 - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */

package man

import (
	"fmt"
	"os"
	"path/filepath"
)

func ManPath(installType string) (string, error) {
	abspath, err := filepath.Abs(os.Args[0])
	if err != nil {
		return "", fmt.Errorf("Unable to get path to man files due to `%s`\n", err.Error())
	}

	exedir := filepath.Dir(abspath)

	if installType == "couchbase" {
		return CouchbaseInstallPath(exedir), nil
	} else {
		return StandaloneInstallPath(exedir), nil
	}
}
