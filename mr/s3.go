package mr

import "strings"

func isS3File(filename string) bool {
	return strings.HasPrefix(filename, "s3://")
}
