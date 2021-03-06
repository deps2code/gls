package gls

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
)

var ctx = context.Background()

type GLSRedisContext struct {
	redisClient *redis.Client
	RedisDB     RedisStorageOps
}

var RedisContext *GLSRedisContext

type RedisStorageOps interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(ctx context.Context, key string) *redis.StringCmd
}

func NewRedisClient() (*redis.Client, error) {
	opt := &redis.Options{
		DialTimeout:  time.Duration(viper.GetInt("database.redis.dialTimeout")) * time.Second,
		ReadTimeout:  time.Duration(viper.GetInt("database.redis.readTimeout")) * time.Second,
		WriteTimeout: time.Duration(viper.GetInt("database.redis.writeTimeout")) * time.Second,
		Addr:         viper.GetString("database.redis.address"),
		Password:     viper.GetString("database.redis.password"),
		DB:           viper.GetInt("database.redis.databaseName"),
	}

	client := redis.NewClient(opt)
	err := client.Ping(ctx).Err()
	return client, err
}

func InitRedisDB() error {
	RedisContext = new(GLSRedisContext)
	redisClient, err := NewRedisClient()
	if err != nil {
		return err
	}
	fmt.Println("Connected to redis successfully")
	RedisContext.RedisDB = redisClient
	RedisContext.redisClient = redisClient
	return nil
}
