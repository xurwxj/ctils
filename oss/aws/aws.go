package aws

import (
	"bytes"
	"errors"
	"fmt"
	"mime/multipart"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	s3sses "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/xurwxj/ctils/oss/utils"
	"github.com/xurwxj/viper"
)

// AWSPutFile need config:
// 需要config:
// "oss": {
//     "cloud": "aws",
//     "xxx": {
//       "endpoint": "xxx",
//       "accessKey": "xxx",
//       "accessSecret": "xxx",
//   	 "bucket": {
//  	    "data": "xxx",
//  	    "pub": "xxx"
// 		  }
//     },
//     "xxx": {
//       "endpoint": "xxx",
//       "accessKey": "xxx",
//       "accessSecret": "xxx",
//   	 "bucket": {
//  	    "data": "xxx",
//  	    "pub": "xxx"
// 		  }
//     }
//   },
func AWSPutFile(prefer, dfsID, bucketType string, c *multipart.FileHeader) error {
	if prefer == "" {
		prefer = "default"
	}
	if bucketType == "" {
		bucketType = "data"
	}
	filePerm := "authenticated-read"
	if bucketType == "pub" {
		filePerm = "public-read"
	}
	fileName := dfsID[strings.LastIndex(dfsID, "/"):]
	if c.Filename != "" {
		fileName = c.Filename
	}

	endpoint := viper.GetString(fmt.Sprintf("oss.%s.endpoint", prefer))
	accessKey := viper.GetString(fmt.Sprintf("oss.%s.accessKey", prefer))
	accessSecret := viper.GetString(fmt.Sprintf("oss.%s.accessSecret", prefer))
	if endpoint == "" || accessKey == "" || accessSecret == "" {
		return fmt.Errorf("authParamsErr")
	}

	creds := credentials.NewStaticCredentials(accessKey, accessSecret, "")
	_, err := creds.Get()
	if err != nil {
		return fmt.Errorf("authErr: %s", err)
	}
	sess := s3sses.Must(s3sses.NewSession(aws.NewConfig().WithRegion(endpoint).WithCredentials(creds)))
	uploader := s3manager.NewUploader(sess)

	f, err := c.Open()
	if err != nil {
		return err
	}
	defer f.Close()

	ct := "application/octet-stream"
	cts, has := c.Header["Content-Type"]
	if has && len(cts) > 0 {
		ct = cts[0]
	}
	if _, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:             aws.String(viper.GetString(fmt.Sprintf("oss.%s.bucket.%s", prefer, bucketType))),
		Key:                aws.String(dfsID),
		ContentType:        aws.String(ct),
		ACL:                aws.String(filePerm),
		ContentDisposition: aws.String(fmt.Sprintf("filename=%s", fileName)),
		Body:               f,
	}); err != nil {
		return fmt.Errorf("uploadErr: %s", err)
	}

	return nil
}

// AWSPutByteFile need config:
// 需要config:
// "oss": {
//     "cloud": "aws",
//     "default": {
//       "endpoint": "xxx",
//       "accessKey": "xxx",
//       "accessSecret": "xxx",
//   	 "bucket": {
//  	    "data": "xxx",
//  	    "pub": "xxx"
// 		  }
//     },
//     "xxx": {
//       "endpoint": "xxx",
//       "accessKey": "xxx",
//       "accessSecret": "xxx",
//   	 "bucket": {
//  	    "data": "xxx",
//  	    "pub": "xxx"
// 		  }
//     }
//   },
func AWSPutByteFile(prefer, dfsID, bucketType string, o map[string]string, c []byte) error {
	if prefer == "" {
		prefer = "default"
	}
	if bucketType == "" {
		bucketType = "data"
	}
	filePerm := "authenticated-read"
	if bucketType == "pub" {
		filePerm = "public-read"
	}
	contentType := "application/octet-stream"
	fileName := dfsID[strings.LastIndex(dfsID, "/"):]
	for k, v := range o {
		switch k {
		case "contentDisposition":
			fileName = v
		case "contentType":
			contentType = v
		}
	}
	endpoint := viper.GetString(fmt.Sprintf("oss.%s.endpoint", prefer))
	accessKey := viper.GetString(fmt.Sprintf("oss.%s.accessKey", prefer))
	accessSecret := viper.GetString(fmt.Sprintf("oss.%s.accessSecret", prefer))
	if endpoint == "" || accessKey == "" || accessSecret == "" {
		return fmt.Errorf("authParamsErr")
	}

	creds := credentials.NewStaticCredentials(accessKey, accessSecret, "")
	_, err := creds.Get()
	if err != nil {
		return fmt.Errorf("authErr: %s", err)
	}
	sess := s3sses.Must(s3sses.NewSession(aws.NewConfig().WithRegion(endpoint).WithCredentials(creds)))
	uploader := s3manager.NewUploader(sess)

	if _, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:             aws.String(viper.GetString(fmt.Sprintf("oss.%s.bucket.%s", prefer, bucketType))),
		Key:                aws.String(dfsID),
		ContentType:        aws.String(contentType),
		ACL:                aws.String(filePerm),
		ContentDisposition: aws.String(fmt.Sprintf("filename=%s", fileName)),
		Body:               bytes.NewReader(c),
	}); err != nil {
		return fmt.Errorf("uploadErr: %s", err)
	}

	return nil
}

// AWSGetTempDownURLFileName get temp download url from aws s3
func AWSGetTempDownURLFileName(bucketName, dfsID string, expires int64) (map[string]string, error) {
	var endPoint string
	rs := make(map[string]string)
	prefer, bn := utils.GetPreferByBucketName(bucketName)
	if bn != "" && prefer != "" {
		endPoint = viper.GetString(fmt.Sprintf("oss.%s.endpoint", prefer))
		accessKey := viper.GetString(fmt.Sprintf("oss.%s.accessKey", prefer))
		accessSecret := viper.GetString(fmt.Sprintf("oss.%s.accessSecret", prefer))
		sess, err := s3sses.NewSession(&aws.Config{
			Region:      aws.String(endPoint),
			Credentials: credentials.NewStaticCredentials(accessKey, accessSecret, ""),
		},
		)
		if err != nil {
			return rs, err
		}

		svc := s3.New(sess)
		req, ib := svc.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(bn),
			Key:    aws.String(dfsID),
		})
		urlStr, err := req.Presign(time.Duration(expires) * time.Second)
		if err != nil {
			return rs, err
		}
		rs["fileName"] = dfsID[strings.LastIndex(dfsID, "/"):]
		cds := *ib.ContentDisposition
		if cdvs := strings.Split(cds, "="); len(cdvs) == 2 {
			rs["fileName"] = strings.TrimSpace(cdvs[1])
		}
		rs["url"] = urlStr
		return rs, nil
	}

	return rs, errors.New("unknownErr")
}

// AWSGetFile get file bytes, filename,content-type, size from aws s3
func AWSGetFile(bucketName, dfsID string) ([]byte, string, string, int64, error) {
	var endPoint string
	prefer, bn := utils.GetPreferByBucketName(bucketName)
	if bn != "" && prefer != "" {
		endPoint = viper.GetString(fmt.Sprintf("oss.%s.endpoint", prefer))
		accessKey := viper.GetString(fmt.Sprintf("oss.%s.accessKey", prefer))
		accessSecret := viper.GetString(fmt.Sprintf("oss.%s.accessSecret", prefer))
		sess, err := s3sses.NewSession(&aws.Config{
			Region:      aws.String(endPoint),
			Credentials: credentials.NewStaticCredentials(accessKey, accessSecret, ""),
		},
		)
		if err != nil {
			return nil, "", "", 0, err
		}

		svc := s3.New(sess)
		it := &s3.GetObjectInput{
			Bucket: aws.String(bn),
			Key:    aws.String(dfsID),
		}
		_, ib := svc.GetObjectRequest(it)
		fileName := dfsID[strings.LastIndex(dfsID, "/"):]
		cds := *ib.ContentDisposition
		if cdvs := strings.Split(cds, "="); len(cdvs) == 2 {
			fileName = strings.TrimSpace(cdvs[1])
		}
		buff := &aws.WriteAtBuffer{}
		downloader := s3manager.NewDownloader(sess)
		tb, err := downloader.Download(buff, it)
		if tb == 0 || err != nil {
			if err != nil {
				return nil, "", "", 0, err
			}
			return nil, "", "", 0, errors.New("emptyErr")
		}
		return buff.Bytes(), fileName, *ib.ContentType, *ib.ContentLength, nil
	}

	return nil, "", "", 0, errors.New("bucketNotExist")
}
