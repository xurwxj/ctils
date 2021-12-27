package mail

import (
	"encoding/base64"

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
	subject = "=?UTF-8?B?" + base64.StdEncoding.EncodeToString([]byte(subject)) + "?="
	switch cloud {
	case "aliyun":
		return AliSendMail(to, fromUser, subject, body, mailType)
	case "aws":
		return AwsSendMail(to, fromUser, subject, body, mailType)
	default:
		return net.SendMail(to, fromUser, subject, body, mailType)
	}
}
