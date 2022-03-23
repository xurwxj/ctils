package sms

import (
	"errors"
	"time"

	json "github.com/json-iterator/go"

	openapi "github.com/alibabacloud-go/darabonba-openapi/client"
	dysmsapi20170525 "github.com/alibabacloud-go/dysmsapi-20170525/v2/client"
	"github.com/alibabacloud-go/tea/tea"
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

func FetchSMSSendResult(phone string) (errSMS []SMSERRObj, err error) {
	config := &openapi.Config{
		// 您的AccessKey ID
		AccessKeyId: tea.String(viper.GetString("sms.accessKey")),
		// 您的AccessKey Secret
		AccessKeySecret: tea.String(viper.GetString("sms.accessSecret")),
	}
	// 访问的域名
	config.Endpoint = tea.String(viper.GetString("sms.domain"))
	client, err := dysmsapi20170525.NewClient(config)
	if err != nil {
		return
	}
	req := &dysmsapi20170525.QuerySendDetailsRequest{
		PhoneNumber: tea.String(phone),
		SendDate:    tea.String(time.Now().UTC().AddDate(0, 0, -1).Format("20060102")),
		PageSize:    tea.Int64(50),
		CurrentPage: tea.Int64(1),
	}
	// fmt.Println(*req.SendDate)
	result, err := client.QuerySendDetails(req)
	if err != nil {
		return
	}
	for _, s := range result.Body.SmsSendDetailDTOs.SmsSendDetailDTO {
		if *s.SendStatus != 3 && *s.ErrCode != "DELIVRD" {
			if errSMS == nil {
				errSMS = make([]SMSERRObj, 0)
			}
			errSMS = append(errSMS, SMSERRObj{
				Code:       *s.ErrCode,
				TemplateID: *s.TemplateCode,
				Phone:      *s.PhoneNum,
				SendOn:     *s.SendDate,
			})
		}
	}
	return
}

type SMSERRObj struct {
	Code       string `json:"code"`
	TemplateID string `json:"templateID"`
	Phone      string `json:"phone"`
	SendOn     string `json:"sendOn"`
}

type aliERR struct {
	Code string `json:"Code"`
}

// ParseAliSMSSendERR parse error code
func ParseAliSMSSendERR(err error) (code string) {
	var rs aliERR
	if jerr := json.Unmarshal([]byte(err.Error()), &rs); jerr == nil {
		return rs.Code
	}
	return
}
