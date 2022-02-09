package redissess

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"github.com/xurwxj/ctils/log"
	"github.com/xurwxj/viper"
)

// SESSRedisDriver implement in redis
type SESSRedisDriver struct {
	RD *redis.Client
}

// InitRedis init redis instance
func InitRedis() SESSRedisDriver {
	fmt.Println("starting init redis....")
	redisAddr := viper.GetString("session.addr")
	if redisAddr == "" {
		log.Log.Err(errors.WithStack(fmt.Errorf("noRedisAddr"))).Msg("InitRedis")
		os.Exit(1)
	}
	redisPwd := viper.GetString("session.passwd")
	dbStart := viper.GetInt("session.db")
	poolSize := viper.GetInt("session.max")
	if poolSize == 0 {
		poolSize = 10
	}
	rd := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPwd,
		DB:       dbStart,
		PoolSize: poolSize,
	})
	if rd == nil {
		fmt.Println("err init redis....")
		log.Log.Err(errors.WithStack(fmt.Errorf("nilRD"))).Msg("InitRedis:NewClient")
		os.Exit(1)
	}
	fmt.Println("done init redis....")
	return SESSRedisDriver{
		RD: rd,
	}
}

// Close close redis instance when exit
func (d SESSRedisDriver) Close() {
	if err := d.RD.Close(); err != nil {
		log.Log.Err(errors.WithStack(err)).Msg("SESSRedisDriver:Close")
	}
}

// SetExpireSession set expirable session
func (d SESSRedisDriver) SetExpireSession(key string, val interface{}, expire int) error {
	buf, err := json.Marshal(val)
	if err != nil {
		return err
	}
	err = d.RD.Set(key, buf, time.Hour*time.Duration(expire)).Err()
	if err != redis.Nil && err != nil {
		return err
	}
	return nil
}

// SetCommonSession set common session
func (d SESSRedisDriver) SetCommonSession(key string, val interface{}) error {
	buf, err := json.Marshal(val)
	if err != nil {
		return err
	}
	err = d.RD.Set(key, buf, 0).Err()
	if err != redis.Nil && err != nil {
		return err
	}
	return nil
}

// GetCommonSession get common session byte or error by key name
func (d SESSRedisDriver) GetCommonSession(key string) ([]byte, error) {
	rs, err := d.RD.Get(key).Bytes()
	if err == redis.Nil {
		err = nil
	}
	return rs, err
}

func (d SESSRedisDriver) RedisLockRefresh(redisKey string, expiration time.Duration) (succ bool) {
	result := d.RD.SetNX(redisKey, 1, expiration)
	return result.Val()
}
