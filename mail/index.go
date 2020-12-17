package mail

import (
	"github.com/xurwxj/gtils/net"
	"github.com/xurwxj/viper"
)

// SendMail need config:
// "email": {
// 		"cloud": "", // aliyun/aws
// 		"account": "xxx@xxx.com",
// 		"accessKey": "xxx",
// 		"accessSecret": "xxx",
// 		"region": "xxx",
// 		"pwd": "xxx",
// 		"host": "smtpdm.xxx.com:25"
//   },
func SendMail(to, fromUser, subject, body, mailType string) error {
	cloud := viper.GetString("email.cloud")
	switch cloud {
	case "aliyun":
		return AliSendMail(to, fromUser, subject, body, mailType)
	case "aws":
		return AwsSendMail(to, fromUser, subject, body, mailType)
	default:
		return net.SendMail(to, fromUser, subject, body, mailType)
	}
}
