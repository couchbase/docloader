package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/couchbase/cbflag"
	"github.com/couchbase/clog"
	"github.com/couchbase/docloader"
	"github.com/couchbase/docloader/man"
)

var installType string = "default"

type Context struct {
	host     string
	username string
	password string
	bucket   string
	quota    int
	dataset  string
	threads  int
	verbose  bool
}

func (c *Context) Run() {
	if !c.verbose {
		clog.SetFlags(0)
		clog.SetOutput(ioutil.Discard)
	}

	loader, err := docloader.CreateJsonSampleImporter(c.host, c.username, c.password,
		c.dataset)
	if err != nil {
		clog.Error(err)
		fmt.Printf("Failed to load data, see log for details\n")
		os.Exit(1)
	}

	phaseErrors := make([]string, 0)
	if !loader.CreateBucket(c.bucket, c.quota) {
		loader.Close()
		fmt.Printf("Bucket creation failed, see log for details\n")
		os.Exit(1)
	}

	if !loader.Views(c.bucket) {
		phaseErrors = append(phaseErrors, "view creation")
	}

	if !loader.Queries(c.bucket) {
		phaseErrors = append(phaseErrors, "index creation")
	}

	if !loader.IterateDocs(c.bucket, c.threads) {
		phaseErrors = append(phaseErrors, "data creation")
	}

	loader.Close()

	if len(phaseErrors) > 0 {
		msg := "Errors occurred during the %s phase%s. See logs for details.\n"
		phases := ""
		for i := 0; i < len(phaseErrors); i++ {
			if i == 0 {
				phases += phaseErrors[i]
			} else if i == len(phaseErrors)-1 {
				phases += " and " + phaseErrors[i]
			} else {
				phases += ", " + phaseErrors[i]
			}
		}

		plural := ""
		if len(phaseErrors) > 1 {
			plural = "s"
		}

		fmt.Printf(msg, phases, plural)
		os.Exit(1)
	}

	fmt.Printf("Data loaded succesfully\n")
	os.Exit(0)
}

func main() {
	// TODO: Write up the man page
	// TODO: Make sure things work on a directory
	// TODO: Add a cmake file

	allow_skip_auth := false
	if os.Getenv("CBDOCLOADER_SKIP_AUTH") == "true" {
		allow_skip_auth = true
	}

	manPath, err := man.ManPath(installType)
	if err != nil {
		clog.Error(err)
		os.Exit(1)
	}

	ctx := &Context{}

	cmdline := &cbflag.CLI{
		Name:    "cbdocloader",
		Desc:    "Imports sample data into Couchbase",
		ManPath: manPath,
		ManPage: man.DocloaderManual(),
		Run:     ctx.Run,
		Flags: []*cbflag.Flag{
			cbflag.HostFlag( // Specified as -c or --cluster
				/* Destination  */ &ctx.host,
				/* Default      */ "",
				/* Deprecated   */ []string{"n"},
				/* Required     */ true,
				/* Hidden       */ false,
			),
			cbflag.UsernameFlag( // Specified as -u or --username
				/* Destination  */ &ctx.username,
				/* Default      */ "",
				/* Deprecated   */ []string{},
				/* Required     */ !allow_skip_auth,
				/* Hidden       */ false,
			),
			cbflag.PasswordFlag( // Specified as -p or --pasword
				/* Destination  */ &ctx.password,
				/* Default      */ "",
				/* Deprecated   */ []string{},
				/* Required     */ !allow_skip_auth,
				/* Hidden       */ false,
			),
			cbflag.StringFlag(
				/* Destination  */ &ctx.bucket,
				/* Default      */ "",
				/* Short Option */ "b",
				/* Long Option  */ "bucket",
				/* Env Variable */ "",
				/* Usage        */ "The bucket name to load the json data into",
				/* Deprecated   */ []string{},
				/* Validator    */ nil,
				/* Required     */ true,
				/* Hidden       */ false,
			),
			cbflag.IntFlag(
				/* Destination  */ &ctx.quota,
				/* Default      */ 100,
				/* Short Option */ "m",
				/* Long Option  */ "bucket-quota",
				/* Env Variable */ "",
				/* Usage        */ "The bucket memory quota",
				/* Deprecated   */ []string{"s"},
				/* Validator    */ nil,
				/* Required     */ true,
				/* Hidden       */ false,
			),
			cbflag.StringFlag(
				/* Destination  */ &ctx.dataset,
				/* Default      */ "",
				/* Short Option */ "d",
				/* Long Option  */ "dataset",
				/* Env Variable */ "",
				/* Usage        */ "The location of the json data",
				/* Deprecated   */ []string{},
				/* Validator    */ nil,
				/* Required     */ true,
				/* Hidden       */ false,
			),
			cbflag.IntFlag(
				/* Destination  */ &ctx.threads,
				/* Default      */ 1,
				/* Short Option */ "t",
				/* Long Option  */ "threads",
				/* Env Variable */ "",
				/* Usage        */ "The amount of parallelism use (Default is 1)",
				/* Deprecated   */ []string{},
				/* Validator    */ nil,
				/* Required     */ false,
				/* Hidden       */ false,
			),
			cbflag.BoolFlag(
				/* Destination  */ &ctx.verbose,
				/* Default      */ false,
				/* Short Option */ "v",
				/* Long Option  */ "verbose",
				/* Env Variable */ "",
				/* Usage        */ "Enable logging to stdout",
				/* Deprecated   */ []string{},
				/* Hidden       */ false,
			),
		},
		Writer: os.Stdout,
	}

	args := compatMode(os.Args)
	cmdline.Parse(args)
}
