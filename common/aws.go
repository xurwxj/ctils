package common

import "time"

type CreateMultipartUploadOutput struct {
	AbortDate               time.Time `json:"abortDate"`
	AbortRuleId             string    `json:"abortRuleId"`
	Bucket                  string    `json:"bucket"`
	BucketKeyEnabled        bool      `json:"bucketKeyEnabled"`
	Key                     string    `json:"key"`
	RequestCharged          string    `json:"requestCharged"`
	SSECustomerAlgorithm    string    `json:"sSECustomerAlgorithm"`
	SSECustomerKeyMD5       string    `json:"sSECustomerKeyMD5"`
	SSEKMSEncryptionContext string    `json:"sSEKMSEncryptionContext"`
	SSEKMSKeyId             string    `json:"sSEKMSKeyId"`
	ServerSideEncryption    string    `json:"serverSideEncryption"`
	UploadId                string    `json:"uploadId"`
}

type CompletedPart struct {
	ETag       string `json:"eTag"`
	PartNumber int64  `json:"partNumber"`
}
