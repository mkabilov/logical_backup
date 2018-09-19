package utils

import (
	"crypto/md5"
	"fmt"

	"github.com/ikitiki/logical_backup/pkg/message"
)

func TableDir(tbl message.Identifier) string {
	tblHash := fmt.Sprintf("%x", md5.Sum([]byte(tbl.Sanitize())))

	return fmt.Sprintf("%s/%s/%s/%s/%s.%s", tblHash[0:2], tblHash[2:4], tblHash[4:6], tblHash, tbl.Namespace, tbl.Name)
}
