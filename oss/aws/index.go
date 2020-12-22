package aws

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	s3sses "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/xurwxj/ctils/oss/utils"
	"github.com/xurwxj/viper"
)

var bs map[string]*s3.S3
var chunkBS map[string]int
var chunkIMURS map[string]int
var imurs map[string]*s3.CreateMultipartUploadOutput
var chunkParts map[string]int
var completedParts map[string][]*s3.CompletedPart

func init() {
	fmt.Println("aws s3 upload init() starting...")
	if bs == nil || len(bs) < 1 {
		bs = make(map[string]*s3.S3)
	}
	if chunkBS == nil || len(chunkBS) < 1 {
		chunkBS = make(map[string]int)
	}
	if imurs == nil || len(imurs) < 1 {
		imurs = make(map[string]*s3.CreateMultipartUploadOutput)
	}
	if chunkIMURS == nil || len(chunkIMURS) < 1 {
		chunkIMURS = make(map[string]int)
	}
	if chunkParts == nil || len(chunkParts) < 1 {
		chunkParts = make(map[string]int)
	}
	if completedParts == nil || len(completedParts) < 1 {
		completedParts = make(map[string][]*s3.CompletedPart, 1)
	}
}

// ChunkUploadGetStream chunk upload for get
// 400 NotExist client should call post to upload this part
// 200 OK client need to ignore this part
// 300 dfsID caller should do other thing, then client can get success upload infor
// 500 client need to upload whole file again
func ChunkUploadGetStream(userID, prefer, cloud string, chunk utils.ChunksObj) (utils.ChunksObj, int, string, error) {
	dfsID := utils.SetMultiPartDfsID(userID, cloud, chunk)
	if !checkPartNumberUploaded(chunk.ChunkNumber, dfsID) {
		return chunk, 400, "NotExist", nil
	}
	if checkAllPartsUploaded(chunk.TotalChunks, dfsID) {
		return completeChunksUpload(userID, prefer, dfsID, chunk)
	}
	return chunk, 200, "OK", nil
}

// ChunkUploadPostStream chunk upload for post
// 400 NotExist client should call post to upload this part
// 200 OK client need to ignore this part
// 300 dfsID caller should do other thing, then client can get success upload infor
// 500 client need to upload whole file again
func ChunkUploadPostStream(userID, prefer, cloud string, chunk utils.ChunksObj, fileChunk *multipart.FileHeader) (utils.ChunksObj, int, string, error) {
	dfsID := utils.SetMultiPartDfsID(userID, cloud, chunk)
	b, err := getBucketInstance(prefer, chunk.Bucket, dfsID, chunk.ChunkNumber)
	if err != nil {
		return chunk, 400, "NotExist", err
	}
	imur, err := getIMURS(prefer, chunk.Bucket, dfsID, chunk.Filename, chunk.ChunkNumber, b)
	if err != nil {
		return chunk, 400, "NotExist", err
	}
	f, err := fileChunk.Open()
	if err != nil {
		return chunk, 400, "NotExist", err
	}
	defer f.Close()
	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, f); err != nil {
		return chunk, 400, "NotExist", err
	}

	fileBytes := buf.Bytes()

	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        imur.Bucket,
		Key:           imur.Key,
		PartNumber:    aws.Int64(int64(chunk.ChunkNumber)),
		UploadId:      imur.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	uploadResult, err := b.UploadPart(partInput)
	if err != nil {
		return chunk, 400, "NotExist", err
	}
	chunkPart := &s3.CompletedPart{
		ETag:       uploadResult.ETag,
		PartNumber: aws.Int64(int64(chunk.ChunkNumber)),
	}
	f.Close()
	err = setCompletePart(chunkPart, dfsID, chunk.ChunkNumber)
	if err != nil {
		return chunk, 400, "NotExist", err
	}
	if checkAllPartsUploaded(chunk.TotalChunks, dfsID) {
		return completeChunksUpload(userID, prefer, dfsID, chunk)
	}
	return chunk, 200, "OK", nil
}

func setCompletePart(part *s3.CompletedPart, dfsID string, chunkNumber int) error {
	cp, has := chunkParts[dfsID]
	if cp > 0 && has {
		time.Sleep(1 * time.Second)
		return setCompletePart(part, dfsID, chunkNumber)
	}
	chunkParts[dfsID] = chunkNumber
	allParts, h := completedParts[dfsID]
	if !h || len(allParts) < 1 {
		allParts = append(allParts, part)
	} else {
		e := false
		for k, p := range allParts {
			if p.PartNumber == part.PartNumber {
				allParts[k] = part
				e = true
			}
		}
		if !e {
			allParts = append(allParts, part)
		}
	}
	if len(allParts) > 1 {
		sort.Slice(allParts, func(i, j int) bool {
			return *allParts[i].PartNumber-*allParts[j].PartNumber < 0
		})
	}
	completedParts[dfsID] = allParts
	delete(chunkParts, dfsID)
	return nil
}

func completeChunksUpload(userID, prefer, dfsID string, chunk utils.ChunksObj) (utils.ChunksObj, int, string, error) {
	allParts, has := completedParts[dfsID]
	if !has || len(allParts) != chunk.TotalChunks {
		go clearInit(dfsID)
		return chunk, 500, "completePartsErr", nil
	}
	b, err := getBucketInstance(prefer, chunk.Bucket, dfsID, chunk.ChunkNumber)
	if err != nil {
		go clearInit(dfsID)
		return chunk, 500, "connectionErr", err
	}
	imur, err := getIMURS(prefer, chunk.Bucket, dfsID, chunk.Filename, chunk.ChunkNumber, b)
	if err != nil {
		go clearInit(dfsID)
		return chunk, 500, "imurErr", err
	}
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   imur.Bucket,
		Key:      imur.Key,
		UploadId: imur.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: allParts,
		},
	}
	_, err = b.CompleteMultipartUpload(completeInput)
	if err != nil {
		go clearInit(dfsID)
		return chunk, 500, "completeErr", err
	}
	objHeader, err := b.HeadObject(&s3.HeadObjectInput{
		Bucket: imur.Bucket,
		Key:    imur.Key,
	})
	if err == nil {
		chunk.ContentType = *objHeader.ContentType
	}
	chunk.Bucket = *imur.Bucket
	chunk.Endpoint = utils.GetEndpointByPrefer(prefer)
	chunk.DfsID = dfsID
	if chunk.DownValidTo > 0 {
		var urlRS map[string]string
		urlRS, err = GetTempDownURLFileName(*imur.Bucket, dfsID, chunk.DownValidTo)
		if url, has := urlRS["url"]; has && url != "" {
			chunk.DownURL = url
		}
	}
	go clearInit(dfsID)
	return chunk, 200, dfsID, err
}

func clearInit(dfsID string) {
	delete(bs, dfsID)
	delete(chunkBS, dfsID)
	delete(imurs, dfsID)
	delete(chunkIMURS, dfsID)
	delete(chunkParts, dfsID)
	delete(completedParts, dfsID)
}

func getIMURS(prefer, bucketType, dfsID, fileName string, chunkNumber int, b *s3.S3) (*s3.CreateMultipartUploadOutput, error) {
	t, h := chunkIMURS[dfsID]
	imur, has := imurs[dfsID]
	if t > 0 && h && !has {
		time.Sleep(1 * time.Second)
		return getIMURS(prefer, bucketType, dfsID, fileName, chunkNumber, b)
	}
	if has && b != nil {
		return imur, nil
	}
	chunkIMURS[dfsID] = chunkNumber
	prefer, bucket := utils.GetByBucketPrefer(prefer, bucketType)
	input := s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(dfsID),
		// ContentType:        aws.String(fileType),
		// ACL:                aws.String("public-read"),
		ContentDisposition: aws.String("filename=" + fileName),
	}
	if bucketType == "pub" {
		input.ACL = aws.String("public-read")
	}
	imur, err := b.CreateMultipartUpload(&input)
	if err != nil {
		return imur, err
	}
	imurs[dfsID] = imur
	return imur, nil
}

func getBucketInstance(prefer, bucketType, dfsID string, chunkNumber int) (*s3.S3, error) {
	t, h := chunkBS[dfsID]
	b, has := bs[dfsID]
	if t > 0 && h && (b == nil || !has) {
		time.Sleep(1 * time.Second)
		return getBucketInstance(prefer, bucketType, dfsID, chunkNumber)
	}
	if has && b != nil {
		return b, nil
	}
	chunkBS[dfsID] = chunkNumber
	endpoint := viper.GetString(fmt.Sprintf("oss.%s.endpoint", prefer))
	accessKey := viper.GetString(fmt.Sprintf("oss.%s.accessKey", prefer))
	accessSecret := viper.GetString(fmt.Sprintf("oss.%s.accessSecret", prefer))
	if endpoint == "" || accessKey == "" || accessSecret == "" {
		return b, fmt.Errorf("configErr")
	}
	creds := credentials.NewStaticCredentials(accessKey, accessSecret, "")
	_, err := creds.Get()
	if err != nil {
		return nil, err
	}
	b = s3.New(s3sses.New(), aws.NewConfig().WithRegion(endpoint).WithCredentials(creds))
	bs[dfsID] = b
	return b, nil
}

func checkAllPartsUploaded(totals int, dfsID string) bool {
	cps, has := completedParts[dfsID]
	if has && len(cps) == totals {
		return true
	}
	return false
}

func checkPartNumberUploaded(chunkNumber int, dfsID string) bool {
	cps, has := completedParts[dfsID]
	if has && len(cps) > 0 {
		for _, cp := range cps {
			if *cp.PartNumber == int64(chunkNumber) {
				return true
			}
		}
	}
	return false
}
