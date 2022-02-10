package sessions

import (
	"time"

	"github.com/rs/zerolog"
	"github.com/xurwxj/ctils/log"
	redissess "github.com/xurwxj/ctils/sessions/redissess"
	"github.com/xurwxj/viper"
)

// SESS session query object
var SESS SESSInterface

// InitSESS init session connection and instance
func InitSESS(logger *zerolog.Logger) redissess.SESSRedisDriver {
	log.Log = logger
	dbType := viper.GetString("session.type")
	if dbType == "" {
		dbType = "redis"
	}
	switch dbType {
	case "redis":
		dricer := redissess.InitRedis()
		SESS = dricer
		return dricer
	}
	return redissess.SESSRedisDriver{RD: nil}
}

// SESSInterface interface for cross sessions
type SESSInterface interface {
	Close()

	SetCommonSession(key string, val interface{}) error
	SetExpireSession(key string, val interface{}, expire int) error
	GetCommonSession(key string) ([]byte, error)

	RedisLockRefresh(redisKey string, expiration time.Duration) (succ bool)
	SetCompletePart(dfsID string, allParts interface{})
	GetCompletePart(dfsID string) (allParts []byte)
	DelCompletePart(dfsID string)

	SetChunkParts(dfsID string, chunkNumber int)
	GetChunkParts(dfsID string) (chunkNumber int)
	DelChunkParts(dfsID string)

	SetChunkBS(dfsID string, chunkNumber int)
	GetChunkBS(dfsID string) (chunkNumber int)
	DelChunkBS(dfsID string)

	SetImurs(dfsID string, imurs interface{})
	GetImurs(dfsID string) (imurs []byte)
	DelImurs(dfsID string)

	SetChunkIMURS(dfsID string, chunkNumber int)
	GetChunkIMURS(dfsID string) (chunkNumber int)
	DelChunkIMURS(dfsID string)

	DelAllParts(dfsID string)
}
