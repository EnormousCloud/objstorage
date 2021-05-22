package localobject

import (
	"bufio"
	"compress/gzip"
	"io/ioutil"
	"os"
	"path/filepath"
)

type service struct {
	localpath string
}

func NewLocalObject(localpath string) *service {
	return &service{
		localpath: localpath,
	}
}
func (s *service) getPath(key string) string {
	return s.localpath + "/" + key + ".gz"
}

func (s *service) Has(key string) bool {
	file, err := os.Open(s.getPath(key))
	if err != nil {
		return false
	}
	defer file.Close()
	return true
}

func (s *service) Get(key string) ([]byte, error) {
	file, err := os.Open(s.getPath(key))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	zr, err := gzip.NewReader(bufio.NewReader(file))
	if err != nil {
		return nil, err
	}
	defer zr.Close()

	return ioutil.ReadAll(zr)
}

func (s *service) Set(key string, buf []byte) error {
	if err := os.MkdirAll(filepath.Dir(s.getPath(key)), os.ModePerm); err != nil {
		return err
	}
	file, err := os.Create(s.getPath(key))
	if err != nil {
		return err
	}
	defer file.Close()

	gz := gzip.NewWriter(file)
	if _, err := gz.Write(buf); err != nil {
		return err
	}
	return gz.Close()
}
