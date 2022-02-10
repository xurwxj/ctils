package redissess

import (
	"encoding/json"
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

func (d SESSRedisDriver) SetChunkParts(dfsID string, chunkNumber int) {
	key := getChunkPartsKey(dfsID)
	//一小时过期
	if err := d.SetExpireSession(key, chunkNumber, 1); err != nil {
		log.Log.Err(err).Msg("SetChunkParts:SetExpireSession")
	}
}

func (d SESSRedisDriver) GetChunkParts(dfsID string) (chunkNumber int) {
	key := getChunkPartsKey(dfsID)
	value, err := d.GetCommonSession(key)
	if err != nil {
		log.Log.Err(err).Msg("GetChunkParts:GetCommonSession")
		return
	}
	if err = json.Unmarshal(value, &chunkNumber); err != nil {
		log.Log.Err(err).Msg("GetChunkParts:Unmarshal")
		return
	}
	return
}

func (d SESSRedisDriver) DelChunkParts(dfsID string) {
	chunkPartsKey := getChunkPartsKey(dfsID)
	d.RD.Del(chunkPartsKey)
}

func (d SESSRedisDriver) SetChunkBS(dfsID string, chunkNumber int) {
	key := getChunkBSKey(dfsID)
	//一小时过期
	if err := d.SetExpireSession(key, chunkNumber, 1); err != nil {
		log.Log.Err(err).Msg("SetChunkBS:SetExpireSession")
	}
}
func (d SESSRedisDriver) GetChunkBS(dfsID string) (chunkNumber int) {
	key := getChunkBSKey(dfsID)
	value, err := d.GetCommonSession(key)
	if err != nil {
		log.Log.Err(err).Msg("GetBS:GetCommonSession")
		return
	}
	if err = json.Unmarshal(value, &chunkNumber); err != nil {
		log.Log.Err(err).Msg("GetChunkParts:Unmarshal")
		return
	}
	return
}
func (d SESSRedisDriver) DelChunkBS(dfsID string) {
	chunkBSsKey := getChunkBSKey(dfsID)
	d.RD.Del(chunkBSsKey)
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

func (d SESSRedisDriver) SetChunkIMURS(dfsID string, chunkNumber int) {
	key := getChunkIMURSKey(dfsID)
	//一小时过期
	if err := d.SetExpireSession(key, chunkNumber, 1); err != nil {
		log.Log.Err(err).Msg("SetChunkBS:SetExpireSession")
	}
}

func (d SESSRedisDriver) GetChunkIMURS(dfsID string) (chunkNumber int) {
	key := getChunkIMURSKey(dfsID)
	value, err := d.GetCommonSession(key)
	if err != nil {
		log.Log.Err(err).Msg("GetBS:GetCommonSession")
		return
	}
	if err = json.Unmarshal(value, &chunkNumber); err != nil {
		log.Log.Err(err).Msg("GetChunkParts:Unmarshal")
		return
	}
	return
}
func (d SESSRedisDriver) DelChunkIMURS(dfsID string) {
	chunkIMURSKey := getChunkIMURSKey(dfsID)
	d.RD.Del(chunkIMURSKey)
}

func (d SESSRedisDriver) DelAllParts(dfsID string) {
	imurkey := getImursKey(dfsID)
	completedPartsKey := getCompletedPartsKey(dfsID)
	chunkPartsKey := getChunkPartsKey(dfsID)
	chunkBSKey := getChunkBSKey(dfsID)
	chunkIMURSKey := getChunkIMURSKey(dfsID)
	pline := d.RD.Pipeline()
	pline.Del(imurkey)
	pline.Del(completedPartsKey)
	pline.Del(chunkPartsKey)
	pline.Del(chunkBSKey)
	pline.Del(chunkIMURSKey)
	cmders, err := pline.Exec()
	if err != nil {
		log.Log.Err(err).Msg("DelAllParts:Exec")
		return
	}
	for _, cmder := range cmders {
		cmd := cmder.(*redis.StringStringMapCmd)
		_, err := cmd.Result()
		if err != nil {
			log.Log.Err(err).Msg("DelAllParts:Result")
		}
	}
}

func getCompletedPartsKey(dfsID string) (key string) {
	return fmt.Sprintf("completed_parts_%s", dfsID)
}

func getChunkPartsKey(dfsID string) (key string) {
	return fmt.Sprintf("chunk_parts_%s", dfsID)
}

func getChunkBSKey(dfsID string) (key string) {
	return fmt.Sprintf("chunk_bs_%s", dfsID)
}

func getImursKey(dfsID string) (key string) {
	return fmt.Sprintf("imurs_%s", dfsID)
}

func getChunkIMURSKey(dfsID string) (key string) {
	return fmt.Sprintf("chunk_imurs_%s", dfsID)
}
