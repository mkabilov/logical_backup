package deltafiles

import (
	"strconv"
	"strings"
)

type DeltaFiles []string

func (d DeltaFiles) Len() int {
	return len(d)
}

func (d DeltaFiles) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d DeltaFiles) Less(i, j int) bool {
	var parts1, parts2 []string

	if strings.Contains(d[i], ".") {
		parts1 = strings.Split(d[i], ".")
	} else {
		parts1 = []string{d[i], "0"}
	}

	if strings.Contains(d[j], ".") {
		parts2 = strings.Split(d[j], ".")
	} else {
		parts2 = []string{d[j], "0"}
	}

	if parts1[0] != parts2[0] {
		return parts1[0] < parts2[0]
	}

	i1, err := strconv.ParseInt(parts1[1], 16, 32)
	if err != nil {
		panic(err)
	}

	i2, err := strconv.ParseInt(parts2[1], 16, 32)
	if err != nil {
		panic(err)
	}

	return i1 < i2
}
