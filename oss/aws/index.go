package aws

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"sort"
	"sync"
	"time"

	json "github.com/json-iterator/go"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	s3sses "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/xurwxj/ctils/common"
	"github.com/xurwxj/ctils/log"
	"github.com/xurwxj/ctils/oss/utils"
	"github.com/xurwxj/ctils/sessions"
	"github.com/xurwxj/viper"
)

type BucketInfo struct {
	OsBucket map[string]*s3.S3
	Lock     sync.Mutex
}

var bsCS BucketInfo

var bs map[string]*s3.S3
var chunkBS map[string]int
var chunkIMURS map[string]int
var imurs map[string]*s3.CreateMultipartUploadOutput
var chunkParts map[string]int
var completedParts map[string][]*s3.CompletedPart

func init() {
	fmt.Println("aws s3 upload init() starting...")
	if bsCS.OsBucket == nil || len(bsCS.OsBucket) < 1 {
		bsCS = BucketInfo{
			OsBucket: make(map[string]*s3.S3),
		}
	}
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

func ChunkUploadGetStreamCS(userID, prefer, cloud string, chunk utils.ChunksObj) (utils.ChunksObj, int, string, error) {
	dfsID := utils.SetMultiPartDfsID(userID, cloud, chunk)
	if !checkPartNumberUploadedCS(chunk.ChunkNumber, dfsID) {
		return chunk, 400, "NotExist", nil
	}
	if checkAllPartsUploadedCS(chunk.TotalChunks, dfsID) {
		return completeChunksUploadCS(userID, prefer, dfsID, chunk)
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

func ChunkUploadPostStreamCS(userID, prefer, cloud string, chunk utils.ChunksObj, fileChunk *multipart.FileHeader) (utils.ChunksObj, int, string, error) {
	dfsID := utils.SetMultiPartDfsID(userID, cloud, chunk)
	b, err := getBucketInstanceCS(prefer, chunk.Bucket, dfsID, chunk.ChunkNumber)
	if err != nil {
		return chunk, 400, "NotExist", err
	}
	imur, err := getIMURSCS(prefer, chunk.Bucket, dfsID, chunk.Filename, chunk.ChunkNumber, b)
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
	allParts, err := setCompletePartCS(chunkPart, dfsID, chunk.ChunkNumber)
	if err != nil {
		return chunk, 400, "NotExist", err
	}
	if len(allParts) == chunk.TotalChunks {
		return completeChunksUploadCS(userID, prefer, dfsID, chunk)
	}
	return chunk, 200, "OK", nil
}
func setCompletePartCS(part *s3.CompletedPart, dfsID string, chunkNumber int) ([]*s3.CompletedPart, error) {
	allParts := make([]*s3.CompletedPart, 0)
	redisLockKey := "multiUpload_setCompletePartCS_" + dfsID
	if !sessions.SESS.RedisLockRefresh(redisLockKey, time.Second*10) {
		// 没拿到锁，重试获取
		time.Sleep(time.Second)
		return setCompletePartCS(part, dfsID, chunkNumber)
	}
	defer sessions.SESS.DelRedisKey(redisLockKey)

	tallParts := sessions.SESS.GetCompletePart(dfsID)
	if len(tallParts) != 0 {
		// 已经被初始化了,直接解析
		if err := json.Unmarshal(tallParts, &allParts); err != nil {
			// 解析失败直接抬走，救不了了
			log.Log.Err(err).Str("tallParts", string(tallParts)).Str("key", dfsID).Msg("setCompletePartCS:Unmarshal")
			return allParts, err
		}
	}

	if len(allParts) < 1 {
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
	redisParts := iniRedisCompletePart(allParts)
	sessions.SESS.SetCompletePart(dfsID, redisParts)
	return allParts, nil
}

func iniRedisCompletePart(allParts []*s3.CompletedPart) (redisParts []common.CompletedPart) {
	redisParts = make([]common.CompletedPart, 0)
	for _, v := range allParts {
		tmp := common.CompletedPart{
			ETag:       "",
			PartNumber: 0,
		}
		if v.ETag != nil {
			tmp.ETag = *v.ETag
		}
		if v.PartNumber != nil {
			tmp.PartNumber = *v.PartNumber
		}
		redisParts = append(redisParts, tmp)
	}
	return
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
	return chunk, 200, "DONE", err
}

func completeChunksUploadCS(userID, prefer, dfsID string, chunk utils.ChunksObj) (utils.ChunksObj, int, string, error) {
	redisLockKey := "multiUpload_completeChunksUploadCS_" + dfsID
	if !sessions.SESS.RedisLockRefresh(redisLockKey, time.Second*10) {
		// 没拿到锁，重试获取
		time.Sleep(time.Second)
		return completeChunksUploadCS(userID, prefer, dfsID, chunk)
	}
	defer sessions.SESS.DelRedisKey(redisLockKey)

	tallParts := sessions.SESS.GetCompletePart(dfsID)
	allParts := make([]*s3.CompletedPart, 0)
	if err := json.Unmarshal(tallParts, &allParts); err != nil {
		log.Log.Err(err).Str("tallParts", string(tallParts)).Str("key", dfsID).Msg("setCompletePartCS:Unmarshal")
	}
	if len(allParts) != chunk.TotalChunks {
		clearInitCS(dfsID)
		return chunk, 500, "completePartsErr", nil
	}
	b, err := getBucketInstanceCS(prefer, chunk.Bucket, dfsID, chunk.ChunkNumber)
	if err != nil {
		clearInitCS(dfsID)
		return chunk, 500, "connectionErr", err
	}
	imur, err := getIMURSCS(prefer, chunk.Bucket, dfsID, chunk.Filename, chunk.ChunkNumber, b)
	if err != nil {
		clearInitCS(dfsID)
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
		clearInitCS(dfsID)
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
	return chunk, 200, "DONE", err
}

func clearInit(dfsID string) {
	delete(bs, dfsID)
	delete(chunkBS, dfsID)
	delete(imurs, dfsID)
	delete(chunkIMURS, dfsID)
	delete(chunkParts, dfsID)
	delete(completedParts, dfsID)
}
func clearInitCS(dfsID string) {
	sessions.SESS.DelAllParts(dfsID)
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

func getIMURSCS(prefer, bucketType, dfsID, fileName string, chunkNumber int, b *s3.S3) (*s3.CreateMultipartUploadOutput, error) {
	imur := &s3.CreateMultipartUploadOutput{}
	redisLockKey := "multiUpload_imurs_" + dfsID
	if !sessions.SESS.RedisLockRefresh(redisLockKey, time.Second*10) {
		// 没拿到锁，重试获取
		time.Sleep(time.Second)
		return getIMURSCS(prefer, bucketType, dfsID, fileName, chunkNumber, b)
	}
	defer sessions.SESS.DelRedisKey(redisLockKey)

	timur := sessions.SESS.GetImurs(dfsID)
	if len(timur) != 0 {
		// 已经被初始化了,直接获取值
		if err := json.Unmarshal(timur, imur); err != nil {
			log.Log.Err(err).Str("timur", string(timur)).Str("key", dfsID).Msg("getIMURSCS:Unmarshal")
			return imur, err
		}
		return imur, nil
	}
	_, bucket := utils.GetByBucketPrefer(prefer, bucketType)
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
	redisImur := initRedisImur(imur)
	sessions.SESS.SetImurs(dfsID, redisImur)
	return imur, nil
}
func initRedisImur(imur *s3.CreateMultipartUploadOutput) (redisImur *common.CreateMultipartUploadOutput) {
	redisImur = &common.CreateMultipartUploadOutput{
		AbortDate:               time.Time{},
		AbortRuleId:             "",
		Bucket:                  "",
		BucketKeyEnabled:        false,
		Key:                     "",
		RequestCharged:          "",
		SSECustomerAlgorithm:    "",
		SSECustomerKeyMD5:       "",
		SSEKMSEncryptionContext: "",
		SSEKMSKeyId:             "",
		ServerSideEncryption:    "",
		UploadId:                "",
	}
	if imur.AbortDate != nil {
		redisImur.AbortDate = *imur.AbortDate
	}
	if imur.AbortRuleId != nil {
		redisImur.AbortRuleId = *imur.AbortRuleId
	}
	if imur.Bucket != nil {
		redisImur.Bucket = *imur.Bucket
	}
	if imur.BucketKeyEnabled != nil {
		redisImur.BucketKeyEnabled = *imur.BucketKeyEnabled
	}
	if imur.Key != nil {
		redisImur.Key = *imur.Key
	}
	if imur.RequestCharged != nil {
		redisImur.RequestCharged = *imur.RequestCharged
	}
	if imur.SSECustomerAlgorithm != nil {
		redisImur.SSECustomerAlgorithm = *imur.SSECustomerAlgorithm
	}
	if imur.SSECustomerKeyMD5 != nil {
		redisImur.SSECustomerKeyMD5 = *imur.SSECustomerKeyMD5
	}
	if imur.SSEKMSEncryptionContext != nil {
		redisImur.SSEKMSEncryptionContext = *imur.SSEKMSEncryptionContext
	}
	if imur.SSEKMSKeyId != nil {
		redisImur.SSEKMSKeyId = *imur.SSEKMSKeyId
	}
	if imur.ServerSideEncryption != nil {
		redisImur.ServerSideEncryption = *imur.ServerSideEncryption
	}
	if imur.UploadId != nil {
		redisImur.UploadId = *imur.UploadId
	}
	return redisImur
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

func getBucketInstanceCS(prefer, bucketType, dfsID string, chunkNumber int) (*s3.S3, error) {
	paramLogs := []interface{}{prefer, bucketType, dfsID, chunkNumber}
	endpoint := viper.GetString(fmt.Sprintf("oss.%s.endpoint", prefer))
	accessKey := viper.GetString(fmt.Sprintf("oss.%s.accessKey", prefer))
	accessSecret := viper.GetString(fmt.Sprintf("oss.%s.accessSecret", prefer))
	if endpoint == "" || accessKey == "" || accessSecret == "" {
		return nil, fmt.Errorf("configErr")
	}
	bsCS.Lock.Lock()
	defer bsCS.Lock.Unlock()
	log.Log.Debug().Interface("paramLogs", paramLogs).Msg("getBucketInstanceCS:OsBucket")
	bucketKey := accessKey + "___" + accessSecret + "___" + endpoint
	b, has := bsCS.OsBucket[bucketKey]
	if has && b != nil {
		return b, nil
	}
	log.Log.Debug().Interface("paramLogs", paramLogs).Msg("getBucketInstanceCS:OsBucket:New")
	creds := credentials.NewStaticCredentials(accessKey, accessSecret, "")
	_, err := creds.Get()
	if err != nil {
		return nil, err
	}
	b = s3.New(s3sses.New(), aws.NewConfig().WithRegion(endpoint).WithCredentials(creds))
	bsCS.OsBucket[bucketKey] = b
	return b, nil
}

func checkAllPartsUploaded(totals int, dfsID string) bool {
	cps, has := completedParts[dfsID]
	if has && len(cps) == totals {
		return true
	}
	return false
}

func checkAllPartsUploadedCS(totals int, dfsID string) bool {
	tallParts := sessions.SESS.GetCompletePart(dfsID)
	allParts := make([]*s3.CompletedPart, 0)
	if err := json.Unmarshal(tallParts, &allParts); err != nil {
		log.Log.Err(err).Str("tallParts", string(tallParts)).Str("key", dfsID).Msg("checkAllPartsUploadedCS:Unmarshal")
	}

	return len(allParts) == totals
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

func checkPartNumberUploadedCS(chunkNumber int, dfsID string) bool {
	cps := sessions.SESS.GetCompletePart(dfsID)
	allParts := make([]*s3.CompletedPart, 0)
	if err := json.Unmarshal(cps, &allParts); err != nil {
		log.Log.Err(err).Str("cps", string(cps)).Str("key", dfsID).Msg("checkPartNumberUploadedCS:Unmarshal")
	}

	if len(allParts) > 0 {
		for _, part := range allParts {
			if *part.PartNumber == int64(chunkNumber) {
				return true
			}
		}
	}
	return false
}
