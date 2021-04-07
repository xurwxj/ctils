package aws

import (
	"bytes"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	s3sses "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/xurwxj/ctils/oss/utils"
	"github.com/xurwxj/gtils/base"
	"github.com/xurwxj/viper"
)

// PutFile need config:
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
func PutFile(prefer, dfsID, bucketType string, chunk utils.ChunksObj, c *multipart.FileHeader) (utils.ChunksObj, error) {
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
		return chunk, fmt.Errorf("authParamsErr")
	}

	creds := credentials.NewStaticCredentials(accessKey, accessSecret, "")
	_, err := creds.Get()
	if err != nil {
		return chunk, fmt.Errorf("authErr: %s", err)
	}
	sess := s3sses.Must(s3sses.NewSession(aws.NewConfig().WithRegion(endpoint).WithCredentials(creds)))
	uploader := s3manager.NewUploader(sess)

	f, err := c.Open()
	if err != nil {
		return chunk, err
	}
	defer f.Close()

	ct := ""
	cts := c.Header.Get("Content-Type")
	if cts != "" {
		ct = cts
	}
	if ct == "" {
		ct = "application/octet-stream"
	}
	bn := viper.GetString(fmt.Sprintf("oss.%s.bucket.%s", prefer, bucketType))
	if _, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:             aws.String(bn),
		Key:                aws.String(dfsID),
		ContentType:        aws.String(ct),
		ACL:                aws.String(filePerm),
		ContentDisposition: aws.String(fmt.Sprintf("filename=%s", fileName)),
		Body:               f,
	}); err != nil {
		return chunk, fmt.Errorf("uploadErr: %s", err)
	} else {
		chunk.ContentType = ct
		chunk.Bucket = bn
		chunk.Endpoint = endpoint
		chunk.DfsID = dfsID
		chunk.TotalSize = c.Size
		if chunk.DownValidTo > 0 {
			var urlRS map[string]string
			urlRS, err = GetTempDownURLFileName(bn, dfsID, chunk.DownValidTo)
			if url, has := urlRS["url"]; has && url != "" {
				chunk.DownURL = url
			}
		}
	}
	return chunk, nil
}

// PutByteFile need config:
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
func PutByteFile(prefer, dfsID, bucketType string, chunk utils.ChunksObj, o map[string]string, c []byte) (utils.ChunksObj, error) {
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
	contentType := ""
	fileName := dfsID[strings.LastIndex(dfsID, "/"):]
	for k, v := range o {
		switch k {
		case "contentDisposition":
			fileName = v
		case "contentType":
			contentType = v
		}
	}
	if contentType == "" {
		contentType = http.DetectContentType(c)
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	endpoint := viper.GetString(fmt.Sprintf("oss.%s.endpoint", prefer))
	accessKey := viper.GetString(fmt.Sprintf("oss.%s.accessKey", prefer))
	accessSecret := viper.GetString(fmt.Sprintf("oss.%s.accessSecret", prefer))
	if endpoint == "" || accessKey == "" || accessSecret == "" {
		return chunk, fmt.Errorf("authParamsErr")
	}

	creds := credentials.NewStaticCredentials(accessKey, accessSecret, "")
	_, err := creds.Get()
	if err != nil {
		return chunk, fmt.Errorf("authErr: %s", err)
	}
	sess := s3sses.Must(s3sses.NewSession(aws.NewConfig().WithRegion(endpoint).WithCredentials(creds)))
	uploader := s3manager.NewUploader(sess)

	bn := viper.GetString(fmt.Sprintf("oss.%s.bucket.%s", prefer, bucketType))
	if _, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:             aws.String(bn),
		Key:                aws.String(dfsID),
		ContentType:        aws.String(contentType),
		ACL:                aws.String(filePerm),
		ContentDisposition: aws.String(fmt.Sprintf("filename=%s", fileName)),
		Body:               bytes.NewReader(c),
	}); err != nil {
		return chunk, fmt.Errorf("uploadErr: %s", err)
	} else {
		chunk.ContentType = contentType
		chunk.Bucket = bn
		chunk.Endpoint = endpoint
		chunk.DfsID = dfsID
		chunk.TotalSize = int64(len(c))
		if chunk.DownValidTo > 0 {
			var urlRS map[string]string
			urlRS, err = GetTempDownURLFileName(bn, dfsID, chunk.DownValidTo)
			if url, has := urlRS["url"]; has && url != "" {
				chunk.DownURL = url
			}
		}
	}

	return chunk, nil
}

// ChunkUpload 文件分块上传
// need config:
// 需要config:
// "oss": {
// "chunkSize":5120000,
//     "cloud": "aliyun",
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
// prefer 如果为空，则取Default
// dfsID 调用SetMultiPartDfsID或是SetDfsID方法生成
// bucketType 如果不传，则表示是data
func ChunkUpload(prefer, dfsID, bucketType, filePath, fileName string) (bn, endpoint string, err error) {
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
	mime := base.GetFileMimeTypeExtByPath(filePath)
	endpoint = viper.GetString(fmt.Sprintf("oss.%s.endpoint", prefer))
	accessKey := viper.GetString(fmt.Sprintf("oss.%s.accessKey", prefer))
	accessSecret := viper.GetString(fmt.Sprintf("oss.%s.accessSecret", prefer))
	if endpoint == "" || accessKey == "" || accessSecret == "" {
		err = fmt.Errorf("authParamsErr")
		return
	}

	creds := credentials.NewStaticCredentials(accessKey, accessSecret, "")
	_, err = creds.Get()
	if err != nil {
		return
	}
	sess := s3sses.Must(s3sses.NewSession(aws.NewConfig().WithRegion(endpoint).WithCredentials(creds)))
	uploader := s3manager.NewUploader(sess)

	bn = viper.GetString(fmt.Sprintf("oss.%s.bucket.%s", prefer, bucketType))
	if bn == "" {
		err = fmt.Errorf("configErr")
		return
	}
	tf, err := os.Open(filePath)
	if err != nil {
		return
	}
	defer tf.Close()
	if fileName == "" {
		fileName = filepath.Base(filePath)
	}
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:             aws.String(bn),
		Key:                aws.String(dfsID),
		ContentType:        aws.String(mime.String()),
		ACL:                aws.String(filePerm),
		ContentDisposition: aws.String(fmt.Sprintf("filename=%s", fileName)),
		Body:               tf,
	}, func(u *s3manager.Uploader) {
		chunkSize := viper.GetInt64("oss.chunkSize")
		if chunkSize < 5242880 {
			chunkSize = 5242880
		}
		u.PartSize = chunkSize
	})
	if err != nil {
		return
	}
	return
}

// GetTempDownURLFileName get temp download url from aws s3
func GetTempDownURLFileName(bucketName, dfsID string, expires int64) (map[string]string, error) {
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
		if ib != nil {
			cds := ib.ContentDisposition
			// fmt.Println("cds: ", cds)
			if cds != nil {
				if cdvs := strings.Split(*cds, "="); len(cdvs) == 2 {
					rs["fileName"] = strings.TrimSpace(cdvs[1])
				}
			}
		}
		rs["url"] = urlStr
		return rs, nil
	}

	return rs, errors.New("unknownErr")
}

// GetFile get file bytes, filename,content-type, size from aws s3
func GetFile(bucketName, dfsID string) ([]byte, string, string, int64, error) {
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
