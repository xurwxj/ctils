package cdn

import (
	"fmt"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/cdn"
	"github.com/xurwxj/viper"
)

// UpdateCDN 刷新或预热
// updateType refresh or push
// 需要config:
// "oss": {
//     "cloud": "aliyun",
//     "xxx": {
//       "endpoint": "xxx",
//       "accessKey": "xxx",
//       "accessSecret": "xxx",
//       "cdnUrl": "xxx"
//     },
//     "xxx": {
//       "endpoint": "xxx",
//       "accessKey": "xxx",
//       "accessSecret": "xxx",
//       "cdnUrl": "xxx"
//     }
//   },
func UpdateCDN(endpoint, path, updateType string) error {
	var c *cdn.Client
	var err error
	accessKey := viper.GetString(fmt.Sprintf("oss.%s.accessKey", endpoint))
	accessSecret := viper.GetString(fmt.Sprintf("oss.%s.accessSecret", endpoint))
	ep := viper.GetString(fmt.Sprintf("oss.%s.ep", endpoint))
	if accessKey == "" || accessSecret == "" || ep == "" {
		return fmt.Errorf("configErr")
	}
	c, err = cdn.NewClientWithAccessKey(ep, accessKey, accessSecret)
	if err != nil {
		return err
	}
	cdnURL := viper.GetString(fmt.Sprintf("oss.%s.cdnUrl", endpoint))
	if cdnURL == "" {
		return fmt.Errorf("cdnURL-%s-notExist", endpoint)
	}
	if updateType == "refresh" {
		cdnReq := cdn.CreateRefreshObjectCachesRequest()
		cdnReq.ObjectPath = fmt.Sprintf("%s%s", cdnURL, path)
		_, err = c.RefreshObjectCaches(cdnReq)
		return err
	}

	if updateType == "push" {
		cdnPushReq := cdn.CreatePushObjectCacheRequest()
		cdnPushReq.ObjectPath = fmt.Sprintf("%s%s", cdnURL, path)
		_, err = c.PushObjectCache(cdnPushReq)
		return err
	}
	return fmt.Errorf("unknownErr")
}
