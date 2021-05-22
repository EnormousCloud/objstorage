package s3object

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type service struct {
	bucket       string
	path         string
	s3Cli        *s3.S3
	s3Downloader *s3manager.Downloader
}

func FromEnv(bucket string, path string) (*service, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	if path == "" {
		path = "/"
	}
	return &service{
		s3Cli:        s3.New(sess),
		s3Downloader: s3manager.NewDownloader(sess),
		bucket:       bucket,
		path:         path,
	}, nil
}

func FromAwsProfile(bucket string, path string, awsProfile string, awsRegion string) (*service, error) {
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewSharedCredentials("", awsProfile),
		Region:      aws.String(awsRegion),
	})
	if err != nil {
		return nil, err
	}
	if path == "" {
		path = "/"
	}
	return &service{
		s3Cli:        s3.New(sess),
		s3Downloader: s3manager.NewDownloader(sess),
		bucket:       bucket,
		path:         path,
	}, nil
}

func (s *service) getPath(key string) string {
	return s.path + key + ".gz"
}

func (s *service) Has(key string) bool {
	_, err := s.s3Cli.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.getPath(key))})
	if err != nil {
		return false
	}
	return true
}

func (s *service) Get(key string) ([]byte, error) {
	buff := &aws.WriteAtBuffer{}
	_, err := s.s3Downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.getPath(key))})
	if err != nil {
		return nil, err
	}
	// fmt.Printf("downloaded %d bytes\n", len(buff.Bytes()))
	zr, err := gzip.NewReader(bytes.NewReader(buff.Bytes()))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return ioutil.ReadAll(zr)
}

func (s *service) Set(key string, buf []byte) error {
	// gzipping contents
	var gzipped bytes.Buffer
	gz := gzip.NewWriter(&gzipped)
	if _, err := gz.Write(buf); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	// fmt.Printf("uploading %d bytes\n", gzipped.Len())
	_, err := s.s3Cli.PutObject(&s3.PutObjectInput{
		Body:               bytes.NewReader(gzipped.Bytes()),
		Bucket:             aws.String(s.bucket),
		Key:                aws.String(s.getPath(key)),
		ContentEncoding:    aws.String("gzip"),
		ContentDisposition: aws.String(fmt.Sprintf("attachment; filename=\"%s.txt.gz\"", s.getPath(key))),
	})
	return err
}
