package mail

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	s3sses "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ses"
	"github.com/xurwxj/gtils/base"
	"github.com/xurwxj/viper"
)

// AwsSendMail need config:
// "email": {
// 		"account": "xxx@xxx.com",
// 		"accessKey": "xxx",
// 		"accessSecret": "xxx",
// 		"region": "xxx"
//   },
func AwsSendMail(to, fromUser, subject, body, mailType string, options ...string) error {
	// The character encoding for the email.
	CharSet := "UTF-8"
	accessKey := viper.GetString("email.accessKey")
	accessSecret := viper.GetString("email.accessSecret")
	region := viper.GetString("email.region")
	user := viper.GetString("email.account")
	for i, o := range options {
		if !base.FindInStringSlice([]string{"", "default"}, o) && i == 0 {
			// fmt.Println(o)
			accessKey = viper.GetString(fmt.Sprintf("email.%s.accessKey", o))
			accessSecret = viper.GetString(fmt.Sprintf("email.%s.accessSecret", o))
			region = viper.GetString(fmt.Sprintf("email.%s.region", o))
			user = viper.GetString(fmt.Sprintf("email.%s.account", o))
		}
	}
	if user == "" || region == "" || accessKey == "" || accessSecret == "" {
		return fmt.Errorf("authParamsErr!")
	}

	creds := credentials.NewStaticCredentials(accessKey, accessSecret, "")
	_, err := creds.Get()
	if err != nil {
		return fmt.Errorf("NewStaticCredentialsErr: %v", err)
	}
	cfg := aws.NewConfig().WithRegion(region).WithCredentials(creds)
	svc := ses.New(s3sses.New(), cfg)
	if fromUser == "" {
		fromUser = user
	}
	var toUser []*string

	for _, t := range strings.Split(to, ";") {
		toUser = append(toUser, aws.String(t))
	}

	// Assemble the email.
	bb := ses.Body{}
	if mailType == "html" {
		bb.Html = &ses.Content{
			Charset: aws.String(CharSet),
			Data:    aws.String(body),
		}
	} else {
		bb.Text = &ses.Content{
			Charset: aws.String(CharSet),
			Data:    aws.String(body),
		}
	}
	input := &ses.SendEmailInput{
		Destination: &ses.Destination{
			CcAddresses: []*string{},
			ToAddresses: toUser,
		},
		Message: &ses.Message{
			Body: &bb,
			Subject: &ses.Content{
				Charset: aws.String(CharSet),
				Data:    aws.String(subject),
			},
		},
		Source: aws.String(fromUser),
		// Uncomment to use a configuration set
		//ConfigurationSetName: aws.String(ConfigurationSet),
	}

	// Attempt to send the email.
	_, err = svc.SendEmail(input)

	// Display error messages if they occur.
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case ses.ErrCodeMessageRejected:
				return fmt.Errorf("%s: %s", ses.ErrCodeMessageRejected, aerr.Error())
			case ses.ErrCodeMailFromDomainNotVerifiedException:
				return fmt.Errorf("%s: %s", ses.ErrCodeMailFromDomainNotVerifiedException, aerr.Error())
			case ses.ErrCodeConfigurationSetDoesNotExistException:
				return fmt.Errorf("%s: %s", ses.ErrCodeConfigurationSetDoesNotExistException, aerr.Error())
			default:
				return fmt.Errorf("AwsSendMailErr: %s", aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			return fmt.Errorf("AwsSendMailErr: %s", err.Error())
		}

	}
	return nil
}
