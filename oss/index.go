package oss

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"mime/multipart"

	"github.com/xurwxj/viper"
)

// PutByteFile auto upload file Byte by cloud
func PutByteFile(prefer, dfsID, bucketType string, o map[string]string, f []byte) error {
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return AliPutByteFile(prefer, dfsID, bucketType, o, f)
	case "aws":
		return AWSPutByteFile(prefer, dfsID, bucketType, o, f)
	}
	return fmt.Errorf("unknownErr")
}

// PutFile auto upload file by cloud
func PutFile(prefer, dfsID, bucketType string, ossFile *multipart.FileHeader) error {
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return AliPutFile(prefer, dfsID, bucketType, ossFile)
	case "aws":
		return AWSPutFile(prefer, dfsID, bucketType, ossFile)
	}
	return fmt.Errorf("unknownErr")
}

// GetFile get file bytes,name,content-type, size auto by cloud
func GetFile(bucketName, dfsID string) ([]byte, string, string, int64, error) {
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return AliGetFile(bucketName, dfsID)
	case "aws":
		return AWSGetFile(bucketName, dfsID)
	}
	return nil, "", "", 0, fmt.Errorf("unknownErr")
}

// GetTempDownURLFileName get file download url auto by cloud
func GetTempDownURLFileName(bucketName, dfsID string, expires int64) (map[string]string, error) {
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return AliGetTempDownURLFileName(bucketName, dfsID, expires)
	case "aws":
		return AWSGetTempDownURLFileName(bucketName, dfsID, expires)
	}
	return map[string]string{}, fmt.Errorf("unknownErr")
}

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

// SetMultiPartDfsID gen unique dfsID by userID, cloud, ChunksObj
func SetMultiPartDfsID(userID, cloud string, obj ChunksObj) string {
	return SetDfsID(userID, obj.Filename, obj.Category, obj.SubCate, obj.RelativePath, obj.Identifier, cloud, obj.TotalSize)
}

// SetDfsID gen unique dfsID by userID, fileName, category, subCategory, relativePath, identifier, cloud, totalSize
func SetDfsID(userID, fileName, category, subCategory, relativePath, identifier, cloud string, totalSize int64) string {
	hasher := md5.New()
	hasher.Write([]byte(fmt.Sprintf("%s%s%s%s%v%s%s%s", userID, fileName, category, subCategory, totalSize, relativePath, identifier, cloud)))
	dfsID := hex.EncodeToString(hasher.Sum(nil))
	dfsID = fmt.Sprintf("%s/%s", category, dfsID)
	if subCategory != "" {
		dfsID = fmt.Sprintf("%s/%s/%s", category, subCategory, dfsID)
	}
	return dfsID
}

// ChunksObj for multipart upload
type ChunksObj struct {
	Category         string `form:"category" json:"category"`
	SubCate          string `form:"subcate" json:"subcate"`
	GEO              string `form:"geo" json:"geo"`
	Bucket           string `form:"bucket" json:"bucket"`
	ChunkNumber      int    `form:"chunkNumber" on:"chunkNumber"`
	Identifier       string `form:"identifier" json:"identifier"`
	Filename         string `form:"filename" json:"filename"`
	RelativePath     string `form:"relativePath" json:"relativePath"`
	CurrentChunkSize int64  `form:"currentChunkSize" json:"currentChunkSize"`
	TotalSize        int64  `form:"totalSize" json:"totalSize"`
	TotalChunks      int    `form:"totalChunks" json:"totalChunks"`
	DownValidTo      int64  `form:"downValidTo" json:"downValidTo"`
}
