package jlib

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

/* redis cache (TBC - wrap around interface - now go-jsonata has access to data outside of the event which is extremely powerful in the right hands) */

// Redis configuration
type redisManager struct {
	Client *redis.Client
	Ctx    context.Context
}

func createRedisManager() (*redisManager, error) {
	r := new(redisManager)
	r.Ctx = context.Background()

	r.Client = redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"),
		Username: os.Getenv("REDIS_USERNAME"),
		Password: os.Getenv("REDIS_PASSWORD"),
	})

	status := r.Client.Ping(r.Ctx)
	if status.Err() != nil {
		return nil, status.Err()
	}

	return r, nil
}

// Set sets key value
func (r *redisManager) Put(bytes []byte, key string) error {
	return r.Client.Set(r.Ctx, key, bytes, 0).Err()
}

// Get gets value
func (r *redisManager) Get(key string) ([]byte, error) {
	return r.Client.Get(r.Ctx, key).Bytes()
}

// Disconnect will disconnect the redis connection
func (r *redisManager) Disconnect() error {
	if r.Client != nil {
		if err := r.Client.Close(); err != nil {
			return err
		}
	}

	return nil
}

func Get(collection, key string) (interface{}, error) {
	r, err := createRedisManager()
	if err != nil {
		return nil, err
	}
	defer func() {
		err = r.Disconnect()
	}()

	bytes, err := r.Get(collection + "|" + key)
	if err != nil {
		if err == redis.Nil {
			out := make([]interface{}, 0)
			return out, nil
		}

		return nil, err
	}

	var output interface{}

	err = json.Unmarshal(bytes, &output)
	if err != nil {
		return output, fmt.Errorf("unescape json unmarshal error: %v", err)
	}

	return output, nil
}

func Put(collection, key string, object interface{}) error {
	r, err := createRedisManager()
	if err != nil {
		return err
	}
	defer func() {
		err = r.Disconnect()
	}()

	bytes, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("json marshal error: %v", err)
	}

	return r.Put(bytes, collection+"|"+key)
}
