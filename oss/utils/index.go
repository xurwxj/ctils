package utils

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"

	"github.com/xurwxj/gtils/base"
	"github.com/xurwxj/viper"
)

// GetByBucketPrefer get prefer and bucket by prefer and bucketType
func GetByBucketPrefer(prefer, bucketType string) (string, string) {
	if prefer == "" {
		prefer = "default"
	}
	if bucketType == "" {
		bucketType = "data"
	}
	for k := range viper.GetStringMap("oss") {
		if k == prefer {
			return k, viper.GetString(fmt.Sprintf("oss.%s.bucket.%s", k, bucketType))
		}
	}
	return "", ""
}

// GetEndpointByPrefer get endpoint by prefer
func GetEndpointByPrefer(prefer string) string {
	return viper.GetString(fmt.Sprintf("oss.%s.endpoint", prefer))
}

// GetPreferByBucketName get prefer by bucket name
func GetPreferByBucketName(bucketName string) (string, string) {
	for k := range viper.GetStringMap("oss") {
		for s := range viper.GetStringMap(fmt.Sprintf("oss.%s.bucket", k)) {
			if viper.GetString(fmt.Sprintf("oss.%s.bucket.%s", k, s)) == bucketName {
				return k, bucketName
			}
		}
	}
	return "", ""
}

// GetPreferByCoutryNode get prefer by bucket name
func GetPreferByCoutryNode(countryNode string) string {
	for k := range viper.GetStringMap("oss") {
		if viper.GetString(fmt.Sprintf("oss.%s.countryNode", k)) == countryNode {
			return k
		}
	}
	return ""
}

// GetCoutryCodeByBucketName get oss countryCode by bucket name
func GetCoutryCodeByBucketName(bucketName string) string {
	for k := range viper.GetStringMap("oss") {
		for s := range viper.GetStringMap(fmt.Sprintf("oss.%s.bucket", k)) {
			if viper.GetString(fmt.Sprintf("oss.%s.bucket.%s", k, s)) == bucketName {
				return viper.GetString(fmt.Sprintf("oss.%s.countryNode", k))
			}
		}
	}
	return ""
}

// GetOSSPreferByCountryID get oss prefer region by countryID
func GetOSSPreferByCountryID(countryID, GEO string) string {
	prefer := "default"
	if (base.FindInStringSlice([]string{"cn", "china", "local"}, countryID) || viper.GetString("env") == "dev" || countryID == "") && base.FindInStringSlice([]string{"true", "1", "t"}, GEO) {
		prefer = "cnhz"
	}
	return prefer
}

// GetBucketTypeByNme get pub bucket name by data bucket name under same endpoint
func GetBucketTypeByNme(d string) string {
	for k := range viper.GetStringMap("oss") {
		for s := range viper.GetStringMap(fmt.Sprintf("oss.%s", k)) {
			if s == "bucket" && viper.GetString(fmt.Sprintf("oss.%s.bucket.data", k)) == d {
				return "data"
			}
			if s == "bucket" && viper.GetString(fmt.Sprintf("oss.%s.bucket.pub", k)) == d {
				return "pub"
			}
		}
	}
	return ""
}

// CheckBucketExistByNmeType check bucket name exist or not
func CheckBucketExistByNmeType(targetBucket, bucketType string) bool {
	for k := range viper.GetStringMap("oss") {
		for s := range viper.GetStringMap(fmt.Sprintf("oss.%s", k)) {
			if s == "bucket" {
				for b := range viper.GetStringMap(fmt.Sprintf("oss.%s.bucket", k)) {
					if viper.GetString(fmt.Sprintf("oss.%s.bucket.%s", k, b)) == targetBucket && bucketType == b {
						return true
					}
				}
			}
		}
	}
	return false
}

// CheckBucketExistByNme check bucket name exist or not
func CheckBucketExistByNme(targetBucket string) bool {
	for k := range viper.GetStringMap("oss") {
		for s := range viper.GetStringMap(fmt.Sprintf("oss.%s", k)) {
			if s == "bucket" {
				for b := range viper.GetStringMap(fmt.Sprintf("oss.%s.bucket", k)) {
					if viper.GetString(fmt.Sprintf("oss.%s.bucket.%s", k, b)) == targetBucket {
						return true
					}
				}
			}
		}
	}
	return false
}

// GetCDNURLByBucket get cdnurl by bucket name under same endpoint
func GetCDNURLByBucket(d string) string {
	for k := range viper.GetStringMap("oss") {
		for s := range viper.GetStringMap(fmt.Sprintf("oss.%s.bucket", k)) {
			if viper.GetString(fmt.Sprintf("oss.%s.bucket.%s", k, s)) == d {
				return viper.GetString(fmt.Sprintf("oss.%s.cdnUrl", k))
			}
		}
	}
	return ""
}

// SetMultiPartDfsID gen unique dfsID by userID, cloud, ChunksObj
func SetMultiPartDfsID(userID, cloud string, obj ChunksObj) string {
	return SetDfsID(userID, obj.Filename, obj.Category, obj.SubCate, obj.RelativePath, obj.Identifier, cloud, obj.TotalSize)
}

// SetDfsID gen unique dfsID by userID, fileName, category, subCategory, relativePath, identifier, cloud, totalSize
func SetDfsID(userID, fileName, category, subCategory, relativePath, identifier, cloud string, totalSize int64) string {
	hasher := md5.New()
	hasher.Write([]byte(fmt.Sprintf("%s%s%s%s%v%s%s%s", userID, fileName, category, subCategory, totalSize, relativePath, identifier, cloud)))
	dfsID := hex.EncodeToString(hasher.Sum(nil))
	if subCategory != "" {
		dfsID = fmt.Sprintf("%s/%s/%s", category, subCategory, dfsID)
	} else {
		dfsID = fmt.Sprintf("%s/%s", category, dfsID)
	}
	return dfsID
}

// ChunksObj for multipart upload
type ChunksObj struct {
	Category string `form:"category" query:"category" json:"category" validate:"uploadCateCheck"`
	SubCate  string `form:"subcate" query:"subcate" json:"subcate"`
	GEO      string `form:"geo" query:"geo" json:"geo"`
	// 在开始上传后变更
	Bucket           string `form:"bucket" query:"bucket" json:"bucket"`
	ChunkNumber      int    `form:"chunkNumber" query:"chunkNumber" json:"chunkNumber"`
	Identifier       string `form:"identifier" query:"identifier" json:"identifier"`
	Filename         string `form:"filename" query:"filename" json:"filename"`
	RelativePath     string `form:"relativePath" query:"relativePath" json:"relativePath"`
	CurrentChunkSize int64  `form:"currentChunkSize" query:"currentChunkSize" json:"currentChunkSize"`
	TotalSize        int64  `form:"totalSize" query:"totalSize" json:"totalSize"`
	TotalChunks      int    `form:"totalChunks" query:"totalChunks" json:"totalChunks"`
	DownValidTo      int64  `form:"downValidTo" query:"downValidTo" json:"downValidTo"`

	// 上传完成后变动
	DfsID       string `json:"dfsID"`
	ContentType string `json:"contentType"`
	Endpoint    string `json:"endpoint"`
	DownURL     string `json:"downURL"`
}
