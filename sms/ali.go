package sms

import (
	"encoding/json"
	"errors"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/xurwxj/viper"
)

// AliSMS ("1367898765456", "xurw", "SMS_193245985", "{\"code\":\"8976\"}")
// need config:
// "sms": {
// 		"domain": "xxx",
// 		"version": "xxx",
// 		"apiName": "xxx",
// 		"accessKey": "xxx",
// 		"accessSecret": "xxx",
// 		"zoneID": "xxx"
//   },
func AliSMS(phone, sign, code, param string) error {
	client, err := sdk.NewClientWithAccessKey(viper.GetString("sms.zoneID"), viper.GetString("sms.accessKey"), viper.GetString("sms.accessSecret"))
	if err != nil {
		return err
	}

	request := requests.NewCommonRequest()
	request.Method = "POST"
	request.Domain = viper.GetString("sms.domain")
	request.Version = viper.GetString("sms.version")
	request.ApiName = viper.GetString("sms.apiName")
	request.QueryParams["RegionId"] = viper.GetString("sms.zoneID")
	request.QueryParams["PhoneNumbers"] = phone
	request.QueryParams["SignName"] = sign
	request.QueryParams["TemplateCode"] = code
	request.QueryParams["TemplateParam"] = param

	response, err := client.ProcessCommonRequest(request)
	if err != nil {
		return err
	}
	res := response.GetHttpContentString()
	var s smsBackObj

	err = json.Unmarshal([]byte(res), &s)
	if err == nil && s.Code == "OK" {
		return nil
	}
	return errors.New(res)
}

type smsBackObj struct {
	Code      string `json:"Code"`
	RequestID string `json:"RequestId"`
	Message   string `json:"Message"`
}
