package oss

import (
	"fmt"
	"mime/multipart"

	"github.com/xurwxj/ctils/oss/ali"
	"github.com/xurwxj/ctils/oss/aws"
	"github.com/xurwxj/ctils/oss/utils"
	"github.com/xurwxj/viper"
)

// ChunkUploadGetStream auto upload file chunk check of get method by cloud
func ChunkUploadGetStream(userID, prefer string, chunk utils.ChunksObj) (int, string, error) {
	if prefer == "" {
		prefer = "default"
	}
	if chunk.Bucket == "" {
		chunk.Bucket = "data"
	}
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return ali.ChunkUploadGetStream(userID, prefer, cloud, chunk)
	case "aws":
		return aws.ChunkUploadGetStream(userID, prefer, cloud, chunk)
	}
	return 500, "unknownErr", fmt.Errorf("unknownErr")
}

// ChunkUploadPostStream auto upload file chunk by cloud
func ChunkUploadPostStream(userID, prefer string, chunk utils.ChunksObj, fileChunk *multipart.FileHeader) (int, string, error) {
	if prefer == "" {
		prefer = "default"
	}
	if chunk.Bucket == "" {
		chunk.Bucket = "data"
	}
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return ali.ChunkUploadPostStream(userID, prefer, cloud, chunk, fileChunk)
	case "aws":
		return aws.ChunkUploadPostStream(userID, prefer, cloud, chunk, fileChunk)
	}
	return 500, "unknownErr", fmt.Errorf("unknownErr")
}

// PutByteFile auto upload file Byte by cloud
func PutByteFile(prefer, dfsID, bucketType string, o map[string]string, f []byte) error {
	if prefer == "" {
		prefer = "default"
	}
	if bucketType == "" {
		bucketType = "data"
	}
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return ali.AliPutByteFile(prefer, dfsID, bucketType, o, f)
	case "aws":
		return aws.AWSPutByteFile(prefer, dfsID, bucketType, o, f)
	}
	return fmt.Errorf("unknownErr")
}

// PutFile auto upload file by cloud
func PutFile(prefer, dfsID, bucketType string, ossFile *multipart.FileHeader) error {
	if prefer == "" {
		prefer = "default"
	}
	if bucketType == "" {
		bucketType = "data"
	}
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return ali.AliPutFile(prefer, dfsID, bucketType, ossFile)
	case "aws":
		return aws.AWSPutFile(prefer, dfsID, bucketType, ossFile)
	}
	return fmt.Errorf("unknownErr")
}

// GetFile get file bytes,name,content-type, size auto by cloud
func GetFile(bucketName, dfsID string) ([]byte, string, string, int64, error) {
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return ali.AliGetFile(bucketName, dfsID)
	case "aws":
		return aws.AWSGetFile(bucketName, dfsID)
	}
	return nil, "", "", 0, fmt.Errorf("unknownErr")
}

// GetTempDownURLFileName get file download url auto by cloud
func GetTempDownURLFileName(bucketName, dfsID string, expires int64) (map[string]string, error) {
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return ali.AliGetTempDownURLFileName(bucketName, dfsID, expires)
	case "aws":
		return aws.AWSGetTempDownURLFileName(bucketName, dfsID, expires)
	}
	return map[string]string{}, fmt.Errorf("unknownErr")
}
