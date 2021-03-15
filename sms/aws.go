package sms

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	s3sses "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/xurwxj/viper"
)

// AwsSMS send sms by aws
// phone 8613567876789
// need config:
// "sms": {
// 		"sign": "xxx",
// 		"accessKey": "xxx",
// 		"accessSecret": "xxx",
// 		"region": "xxx"
//   },
func AwsSMS(phone, sign, message string) error {
	accessKey := viper.GetString("sms.accessKey")
	accessSecret := viper.GetString("sms.accessSecret")
	if sign == "" {
		sign = viper.GetString("sms.sign")
	}
	region := viper.GetString("sms.region")
	if region == "" || accessKey == "" || accessSecret == "" {
		return fmt.Errorf("AwsSMSAuthParamsErr")
	}

	sess, err := s3sses.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(accessKey, accessSecret, ""),
	},
	)

	// Create SNS service
	svc := sns.New(sess)

	if sign == "" {
		sign = "FindNow"
	}

	// Pass the phone number and message.
	params := &sns.PublishInput{
		PhoneNumber: aws.String(phone),
		Message:     aws.String(message),
		MessageAttributes: map[string]*sns.MessageAttributeValue{
			"AWS.SNS.SMS.SenderID": &sns.MessageAttributeValue{
				StringValue: aws.String(sign),
				DataType:    aws.String("String"),
			},
			"AWS.SNS.SMS.SMSType": &sns.MessageAttributeValue{
				StringValue: aws.String("Promotional"),
				DataType:    aws.String("String"),
			}},
		// MessageAttributes: map[string]*sns.MessageAttributeValue{"AWS.SNS.SMS.SenderID": &sns.MessageAttributeValue{StringValue: aws.String("Shining3D"), DataType: aws.String("String")}, "AWS.SNS.SMS.SMSType": &sns.MessageAttributeValue{StringValue: aws.String("Transactional"), DataType: aws.String("String")}},
	}
	// params = params.SetMessageAttributes(map[string]*sns.MessageAttributeValue{"AWS.SNS.SMS.SenderID": &sns.MessageAttributeValue{StringValue: aws.String("Shining3D"), DataType: aws.String("String")}})

	// sends a text message (SMS message) directly to a phone number.
	_, err = svc.Publish(params)

	if err != nil {
		return fmt.Errorf("AwsSMSSendErr: %v", err)
	}

	return nil
}
