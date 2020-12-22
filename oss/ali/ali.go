package ali

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/xurwxj/ctils/oss/utils"
	"github.com/xurwxj/viper"
)

func initOss(prefer string) (*oss.Client, error) {
	endpoint := viper.GetString(fmt.Sprintf("oss.%s.endpoint", prefer))
	accessKey := viper.GetString(fmt.Sprintf("oss.%s.accessKey", prefer))
	accessSecret := viper.GetString(fmt.Sprintf("oss.%s.accessSecret", prefer))
	if endpoint == "" || accessKey == "" || accessSecret == "" {
		return nil, fmt.Errorf("configErr")
	}
	client, err := oss.New(endpoint, accessKey, accessSecret)
	if err == nil {
		return client, nil
	}
	return nil, err
}

// InitBucket init bucket connection
// need config:
// 需要config:
// "oss": {
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
func InitBucket(prefer, bucket string) (*oss.Bucket, error) {
	// fmt.Println(prefer)
	c, err := initOss(prefer)
	if err == nil && c != nil {
		b, err := c.Bucket(bucket)
		if err == nil {
			return b, nil
		}
	}
	return nil, err
}

// PutByteFile 用于字节文件上传
// need config:
// 需要config:
// "oss": {
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
func PutByteFile(prefer, dfsID, bucketType string, chunk utils.ChunksObj, o map[string]string, f []byte) (utils.ChunksObj, error) {
	prefer, bn := utils.GetByBucketPrefer(prefer, bucketType)
	if bn != "" && prefer != "" {
		b, err := InitBucket(prefer, bn)
		if err == nil {
			options := initOptions(o)
			err = b.PutObject(dfsID, bytes.NewReader(f), options...)
			if err == nil {
				ct, h := o["contentType"]
				if !h || ct == "" {
					ct = http.DetectContentType(f)
				}
				chunk.ContentType = ct
				chunk.Bucket = bn
				chunk.Endpoint = utils.GetEndpointByPrefer(prefer)
				chunk.DfsID = dfsID
				chunk.TotalSize = int64(len(f))
				if chunk.DownValidTo > 0 {
					var urlRS map[string]string
					urlRS, err = GetTempDownURLFileName(bn, dfsID, chunk.DownValidTo)
					if url, has := urlRS["url"]; has && url != "" {
						chunk.DownURL = url
					}
				}
			}
		}
		return chunk, err
	}
	return chunk, errors.New("bucketNotExist")
}

// PutFile 文件方式上传
// need config:
// 需要config:
// "oss": {
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
func PutFile(prefer, dfsID, bucketType string, chunk utils.ChunksObj, ossFile *multipart.FileHeader) (utils.ChunksObj, error) {
	prefer, bn := utils.GetByBucketPrefer(prefer, bucketType)
	chunk.Bucket = bn
	if bn != "" && prefer != "" {
		b, err := InitBucket(prefer, bn)
		if err == nil {
			options := []oss.Option{
				oss.ContentDisposition(fmt.Sprintf("filename=%s", ossFile.Filename)),
			}
			ct := ossFile.Header.Get("Content-Type")
			if ct != "" {
				options = append(options, oss.ContentType(ct))
			}
			f, err := ossFile.Open()
			if err != nil {
				return chunk, err
			}
			defer f.Close()
			err = b.PutObject(dfsID, f, options...)
			if err != nil {
				return chunk, err
			}
			if err == nil {
				chunk.ContentType = ct
				chunk.Bucket = bn
				chunk.Endpoint = utils.GetEndpointByPrefer(prefer)
				chunk.DfsID = dfsID
				chunk.TotalSize = ossFile.Size
				if chunk.DownValidTo > 0 {
					var urlRS map[string]string
					urlRS, err = GetTempDownURLFileName(bn, dfsID, chunk.DownValidTo)
					if url, has := urlRS["url"]; has && url != "" {
						chunk.DownURL = url
					}
				}
				return chunk, nil
			}
		}
		return chunk, err
	}
	return chunk, errors.New("bucketNotExist")
}

func initOptions(o map[string]string) []oss.Option {
	options := make([]oss.Option, 0)
	for k, v := range o {
		switch k {
		case "contentDisposition":
			options = append(options, oss.ContentDisposition(fmt.Sprintf("filename=%s", v)))
		case "contentType":
			options = append(options, oss.ContentType(v))
		}
	}
	return options
}

// GetTempDownURLFileName get temp download url from oss
// return example:
// {
// 	"url":"xxx",
// 	"fileName":"xxx"
// }
func GetTempDownURLFileName(bucketName, dfsID string, expires int64) (map[string]string, error) {
	var b *oss.Bucket
	var err error
	var endPoint string
	var downURL string
	rs := make(map[string]string)
	prefer, bn := utils.GetPreferByBucketName(bucketName)
	if bn != "" && prefer != "" {
		if b, err = InitBucket(prefer, bn); err != nil {
			return rs, err
		}
		endPoint = viper.GetString(fmt.Sprintf("oss.%s.endpoint", prefer))
		downURL = viper.GetString(fmt.Sprintf("oss.%s.downUrl", prefer))
		if endPoint == "" || downURL == "" {
			return rs, fmt.Errorf("configErr")
		}
		props, err := b.GetObjectDetailedMeta(dfsID)
		if err != nil {
			return rs, err
		}
		// fmt.Println(props)
		if cdv, ok := props["Content-Disposition"]; ok {
			if cdvs := strings.Split(cdv[0], "="); len(cdvs) == 2 {
				rs["fileName"] = strings.TrimSpace(cdvs[1])
			}
		}
		signedURL, err := b.SignURL(dfsID, oss.HTTPGet, expires)
		if err != nil {
			return rs, err
		}
		url := strings.Replace(endPoint, "https://", fmt.Sprintf("%s.", bn), 1)
		rs["url"] = strings.Replace(signedURL, url, downURL, 1)
		return rs, nil
	}

	return rs, errors.New("unknownErr")
}

// GetFile get file bytes, filename,content-type, size from ali oss
func GetFile(bucketName, dfsID string) ([]byte, string, string, int64, error) {
	prefer, bn := utils.GetPreferByBucketName(bucketName)
	if bn != "" && prefer != "" {
		b, err := InitBucket(prefer, bn)
		if err == nil && b != nil {
			isExist, err := b.IsObjectExist(dfsID)
			if !isExist || err != nil {
				if err != nil {
					return nil, "", "", 0, err
				}
				return nil, "", "", 0, fmt.Errorf("notExist")
			}
			props, err := b.GetObjectDetailedMeta(dfsID)
			if err != nil {
				return nil, "", "", 0, err
			}
			file, err := b.GetObject(dfsID)
			if err != nil {
				return nil, "", "", 0, err
			}
			data, err := ioutil.ReadAll(file)
			file.Close()
			if err != nil {
				return nil, "", "", 0, err
			}
			fileName := dfsID[strings.LastIndex(dfsID, "/"):]
			if len(props["Content-Disposition"]) > 0 {
				if cdi := strings.Split(props["Content-Disposition"][0], "="); len(cdi) == 2 {
					fileName = strings.TrimSpace(cdi[1])
				}
			}
			fileSize := int64(0)
			if len(props["Content-Length"]) > 0 {
				fs, err := strconv.ParseInt(props["Content-Length"][0], 10, 64)
				if err == nil {
					fileSize = fs
				}
			}
			ct := "application/octet-stream"
			if len(props["Content-Type"]) > 0 {
				ct = props["Content-Type"][0]
			}
			return data, fileName, ct, fileSize, nil
		}
	}

	return nil, "", "", 0, errors.New("bucketNotExist")
}
