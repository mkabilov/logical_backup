package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/ikitiki/logical_backup/pkg/deltas"
)

const columnWidth = 10

var (
	filePaths []string

	Version  = "devel"
	Revision = "devel"

	GoVersion = runtime.Version()
)

func buildInfo() string {
	return fmt.Sprintf("logical backup delta inspector tool %s git revision %s go version %s", Version, Revision, GoVersion)
}

func init() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "%s\n", buildInfo())
		fmt.Fprintf(os.Stderr, "\nUsage:\n\t%s {deltafile}\n", os.Args[0])
		os.Exit(1)
	}

	filePaths = os.Args[1:]
}

func dumpFile(filepath string) {
	d := deltas.New("", false)
	if err := d.Load(filepath); err != nil {
		fmt.Fprintf(os.Stderr, "could not load file: %v\n", err)
		os.Exit(1)
	}

	for {
		msg, err := d.GetMessage()
		if err == io.EOF {
			break
		}

		msgType := msg.MsgType().String()
		delimiter := strings.Repeat(" ", columnWidth-len(msgType))
		fmt.Printf("%s:%s%s\n", msgType, delimiter, msg.String())
	}

	if err := d.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "could not close file: %v\n", err)
	}
}

func main() {
	for _, filePath := range filePaths {
		dumpFile(filePath)
	}
}
