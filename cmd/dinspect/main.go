package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/ikitiki/logical_backup/pkg/decoder"
)

var (
	filePaths []string

	Version  string
	Revision string

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

func main() {
	for _, filePath := range filePaths {
		fmt.Printf("reading %q segment file: \n\n", filePath)

		fp, err := os.OpenFile(filePath, os.O_RDONLY, os.ModePerm)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not open file: %v\n", err)
			os.Exit(1)
		}
		defer fp.Close()

		lnBuf := make([]byte, 8)
		for {
			if n, err := fp.Read(lnBuf); err == io.EOF && n == 0 {
				break
			} else if err != nil || n != 8 {
				fmt.Fprintf(os.Stderr, "could not read: %v\n", err)
				os.Exit(1)
			}

			ln := binary.BigEndian.Uint64(lnBuf) - 8
			dataBuf := make([]byte, ln)

			if n, err := fp.Read(dataBuf); err == io.EOF && n == 0 {
				break
			} else if err != nil || uint64(n) != ln {
				fmt.Fprintf(os.Stderr, "could not read %d bytes: %v", ln, err)
				os.Exit(1)
			}

			msg, err := decoder.Parse(dataBuf)
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not parse message: %v", err)
				os.Exit(1)
			}

			msgType := msg.MsgType()
			delimiter := strings.Repeat(" ", 10-len(msgType))
			fmt.Printf("%s:%s%s\n", msgType, delimiter, msg.String())
		}
	}
}
