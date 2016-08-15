CMAKE_MINIMUM_REQUIRED (VERSION 2.8)
PROJECT (cbdocloader)

# This project is intended to be built as part of a Couchbase Server build,
# but for nominal support building stand-alone, the following variables
# need to be specified:
#
# CMAKE_MODULE_PATH - should contain a directory containing
#      FindCouchbaseGo.cmake, eg. tlm/cmake/Modules
#
# CMAKE_INSTALL_PREFIX - should point to a directory where the bin/ output
#      directory will be placed
#
# GODEPSDIR - should point to a Go workspace directory containing all
#      transitive Go dependencies
#

INCLUDE (FindCouchbaseGo)

# Get the commit hash
IF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)
  EXECUTE_PROCESS(COMMAND git rev-parse --short HEAD
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                  OUTPUT_VARIABLE _version_hash
                  OUTPUT_STRIP_TRAILING_WHITESPACE)
  IF (_version_hash STREQUAL "" OR _version_hash STREQUAL "HEAD")
    SET(_version_hash "unknown")
  ENDIF (_version_hash STREQUAL "" OR _version_hash STREQUAL "HEAD")
ELSE (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)
  SET(_version_hash "unknown")
ENDIF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/.git)

GoInstall (TARGET cbdocloader PACKAGE github.com/couchbase/docloader/cmd/cbdocloader
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  ${_forestdb_dep}
  CGO_INCLUDE_DIRS ""
  CGO_LIBRARY_DIRS ""
  GOTAGS "forestdb forestdb_default_kvstore"
  LDFLAGS "${_ldflags} -X main.installType=couchbase -X main.versionHash=${_version_hash}"
  INSTALL_PATH bin OUTPUT cbdocloader
  GOVERSION 1.6)

INSTALL(FILES
  examples/beer-sample.zip
  examples/gamesim-sample.zip
  examples/travel-sample.zip
  DESTINATION samples)

IF (WIN32)
  INSTALL(FILES
    docs/html/cbdocloader.html
    DESTINATION share/docs/html)
ELSE ()
  INSTALL(FILES
    man/man1/cbdocloader.1
    DESTINATION share/man/man1)
ENDIF ()
