package redissess

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"

	"github.com/go-redis/redis"
	"github.com/xurwxj/ctils/log"
)

func (d SESSRedisDriver) SetCompletePart(dfsID string, allParts interface{}) {
	key := getCompletedPartsKey(dfsID)
	//一小时过期
	if err := d.SetExpireSession(key, allParts, 1); err != nil {
		log.Log.Err(err).Msg("SetCompletePart:SetExpireSession")
	}
}

func (d SESSRedisDriver) GetCompletePart(dfsID string) (value []byte) {
	key := getCompletedPartsKey(dfsID)

	value, err := d.GetCommonSession(key)
	if err != nil {
		log.Log.Err(err).Msg("GetCompletePart:GetCommonSession")
		return
	}
	return value
}

func (d SESSRedisDriver) DelCompletePart(dfsID string) {
	completedPartsKey := getCompletedPartsKey(dfsID)
	d.RD.Del(completedPartsKey)
}

func (d SESSRedisDriver) SetImurs(dfsID string, imurs interface{}) {
	key := getImursKey(dfsID)
	//一小时过期
	if err := d.SetExpireSession(key, imurs, 1); err != nil {
		log.Log.Err(err).Msg("SetChunkBS:SetExpireSession")
	}
}

func (d SESSRedisDriver) GetImurs(dfsID string) (imurs []byte) {
	key := getImursKey(dfsID)
	imurs, err := d.GetCommonSession(key)
	if err != nil {
		log.Log.Err(err).Msg("GetBS:GetCommonSession")
		return
	}
	return imurs
}
func (d SESSRedisDriver) DelImurs(dfsID string) {
	imursKey := getImursKey(dfsID)
	d.RD.Del(imursKey)
}

func (d SESSRedisDriver) DelAllParts(dfsID string) {
	imurkey := getImursKey(dfsID)
	completedPartsKey := getCompletedPartsKey(dfsID)
	pline := d.RD.Pipeline()
	pline.Del(imurkey)
	pline.Del(completedPartsKey)
	cmders, err := pline.Exec()
	if err != nil {
		log.Log.Err(err).Msg("DelAllParts:Exec")
		return
	}
	for _, cmder := range cmders {
		cmd := cmder.(*redis.IntCmd)
		_, err := cmd.Result()
		if err != nil {
			log.Log.Err(err).Msg("DelAllParts:Result")
		}
	}
}

func getCompletedPartsKey(dfsID string) (key string) {
	dfsID = Md5String(dfsID)
	return fmt.Sprintf("com_parts_%s", dfsID)
}

func getImursKey(dfsID string) (key string) {
	dfsID = Md5String(dfsID)
	return fmt.Sprintf("im_%s", dfsID)
}

func Md5String(dfsID string) string {
	hasher := md5.New()
	hasher.Write([]byte(dfsID))
	return hex.EncodeToString(hasher.Sum(nil))
}
