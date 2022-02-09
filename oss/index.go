package oss

import (
	"fmt"
	"mime/multipart"

	"github.com/xurwxj/ctils/oss/ali"
	"github.com/xurwxj/ctils/oss/aws"
	"github.com/xurwxj/ctils/oss/utils"
	"github.com/xurwxj/viper"
)

// ChunkUpload auto upload local file chunk check of get method by cloud
func ChunkUpload(prefer, dfsID, bucketType, filePath, fileName string) (bn, endpoint string, err error) {
	if prefer == "" {
		prefer = "default"
	}
	if bucketType == "" {
		bucketType = "data"
	}
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return ali.ChunkUpload(prefer, dfsID, bucketType, filePath, fileName)
	case "aws":
		return aws.ChunkUpload(prefer, dfsID, bucketType, filePath, fileName)
	}
	return "", "", fmt.Errorf("unknownErr")
}

// ChunkUploadGetStream auto upload file chunk check of get method by cloud
func ChunkUploadGetStream(userID, prefer string, chunk utils.ChunksObj) (utils.ChunksObj, int, string, error) {
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
	return chunk, 500, "unknownErr", fmt.Errorf("unknownErr")
}

// ChunkUploadGetStreamCS auto upload file chunk check of get method by cloud also concurrent safe
func ChunkUploadGetStreamCS(userID, prefer string, chunk utils.ChunksObj) (utils.ChunksObj, int, string, error) {
	if prefer == "" {
		prefer = "default"
	}
	if chunk.Bucket == "" {
		chunk.Bucket = "data"
	}
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return ali.ChunkUploadGetStreamCS(userID, prefer, cloud, chunk)
	case "aws":
		return aws.ChunkUploadGetStreamCS(userID, prefer, cloud, chunk)
	}
	return chunk, 500, "unknownErr", fmt.Errorf("unknownErr")
}

// ChunkUploadPostStream auto upload file chunk by cloud
func ChunkUploadPostStream(userID, prefer string, chunk utils.ChunksObj, fileChunk *multipart.FileHeader) (utils.ChunksObj, int, string, error) {
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
	return chunk, 500, "unknownErr", fmt.Errorf("unknownErr")
}

// PutByteFile auto upload file Byte by cloud
func PutByteFile(prefer, dfsID string, chunk utils.ChunksObj, o map[string]string, f []byte) (utils.ChunksObj, error) {
	if prefer == "" {
		prefer = "default"
	}
	bucketType := "data"
	if chunk.Bucket != "" {
		bucketType = chunk.Bucket
	}
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return ali.PutByteFile(prefer, dfsID, bucketType, chunk, o, f)
	case "aws":
		return aws.PutByteFile(prefer, dfsID, bucketType, chunk, o, f)
	}
	return chunk, fmt.Errorf("unknownErr")
}

// PutFile auto upload file by cloud
func PutFile(prefer, dfsID string, chunk utils.ChunksObj, ossFile *multipart.FileHeader) (utils.ChunksObj, error) {
	if prefer == "" {
		prefer = "default"
	}
	bucketType := "data"
	if chunk.Bucket != "" {
		bucketType = chunk.Bucket
	}
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return ali.PutFile(prefer, dfsID, bucketType, chunk, ossFile)
	case "aws":
		return aws.PutFile(prefer, dfsID, bucketType, chunk, ossFile)
	}
	return chunk, fmt.Errorf("unknownErr")
}

// GetFile get file bytes,name,content-type, size auto by cloud
func GetFile(bucketName, dfsID string) ([]byte, string, string, int64, error) {
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return ali.GetFile(bucketName, dfsID)
	case "aws":
		return aws.GetFile(bucketName, dfsID)
	}
	return nil, "", "", 0, fmt.Errorf("unknownErr")
}

// GetTempDownURLFileName get file download url auto by cloud
func GetTempDownURLFileName(bucketName, dfsID string, expires int64) (map[string]string, error) {
	cloud := viper.GetString("oss.cloud")
	switch cloud {
	case "aliyun":
		return ali.GetTempDownURLFileName(bucketName, dfsID, expires)
	case "aws":
		return aws.GetTempDownURLFileName(bucketName, dfsID, expires)
	}
	return map[string]string{}, fmt.Errorf("unknownErr")
}
