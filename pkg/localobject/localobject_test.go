package localobject

import (
	"io/ioutil"
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
func TestNewLocalObject(t *testing.T) {
	path, err := ioutil.TempDir("", "localobject")
	require.NoError(t, err)
	defer os.Remove(path)
	obj := NewLocalObject(path)

	value := []byte(RandStringBytesMask(1024 * 100))
	err = obj.Set("testkey", value)
	require.NoError(t, err)
	val, err := obj.Get("testkey")
	require.NoError(t, err)
	assert.Equal(t, val, value)
}
