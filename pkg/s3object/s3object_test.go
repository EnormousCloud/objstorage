package s3object

import (
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
)

func RandStringBytesMask(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; {
		if idx := int(rand.Int63() & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i++
		}
	}
	return string(b)
}

func TestFromAwsProfile(t *testing.T) {
	bucket := os.Getenv("AWS_BUCKET")
	if len(bucket) == 0 {
		t.Skip("AWS_BUCKET env must be configured")
	}
	awsProfile := os.Getenv("AWS_PROFILE")
	if len(bucket) == 0 {
		t.Skip("AWS_PROFILE env must be configured")
	}
	awsRegion := os.Getenv("AWS_REGION")
	if len(bucket) == 0 {
		t.Skip("AWS_REGION env must be configured")
	}
	path := "/"
	obj, err := FromAwsProfile(bucket, path, awsProfile, awsRegion)
	require.NoError(t, err)

	value := []byte(RandStringBytesMask(1024 * 100))
	err = obj.Set("testkey", value)
	require.NoError(t, err)
	val, err := obj.Get("testkey")
	require.NoError(t, err)
	assert.Equal(t, val, value)
}
