package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisStore implements the Storage interface, providing access to storage backed by a Redis instance
type RedisStore struct {
	prefix string
	client *redis.Client
	objSer ObjectSerializer
	objDes ObjectDeserializer
}

// RedisOptions describes options used when creating a new Redis client
type RedisOptions struct {
	// Addr is the host:port to use to connect to Redis
	Addr string
	// DB selects which redis database to select after connecting
	DB int
	// Use the specified Username to authenticate the current connection
	// with one of the connections defined in the ACL list when connecting
	// to a Redis 6.0 instance, or greater, that is using the Redis ACL system.
	Username string
	// Optional password. Must match the password specified in the
	// requirepass server configuration option (if connecting to a Redis 5.0 instance, or lower),
	// or the User Password when connecting to a Redis 6.0 instance, or greater,
	// that is using the Redis ACL system.
	Password string

	ObjectSerializer   ObjectSerializer
	ObjectDeserializer ObjectDeserializer
}

// NewRedisStore creates a new instance of RedisStore, where all operations are namedspaced by prefix. For instance,
// when setting a string with the key `foo` to `bar`, what gets stored in Redis is a string as `{prefix}:foo` with the
// value of `bar.
func NewRedisStore(prefix string, opts *RedisOptions) (*RedisStore, error) {
	objSer := opts.ObjectSerializer
	if objSer == nil {
		objSer = json.Marshal
	}

	objDes := opts.ObjectDeserializer
	if objDes == nil {
		objDes = json.Unmarshal
	}

	o := &redis.Options{
		Addr:     opts.Addr,
		DB:       opts.DB,
		Username: opts.Username,
		Password: opts.Password,
	}

	return &RedisStore{
		prefix: prefix,
		client: redis.NewClient(o),
		objSer: objSer,
		objDes: objDes,
	}, nil
}

func (r *RedisStore) prefixKey(key string) string {
	if r.prefix == "" {
		return key
	}
	return fmt.Sprintf("%s:%s", r.prefix, key)
}

func (r *RedisStore) Exists(ctx context.Context, id string) bool {
	num, err := r.client.Exists(ctx, r.prefixKey(id)).Result()
	if err != nil {
		return false
	}
	return num > 0
}

func (r *RedisStore) Delete(ctx context.Context, id string) error {
	return r.client.Del(ctx, r.prefixKey(id)).Err()
}

func (r *RedisStore) DeleteAll(ctx context.Context) error {
	return r.DeleteMatching(ctx, ".*")
}

func (r *RedisStore) DeleteMatching(ctx context.Context, pattern string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	var toDelete []string

	scan := r.client.Scan(ctx, 0, r.prefixKey("*"), 10)
	if scan.Err() != nil {
		return scan.Err()
	}

	iter := scan.Iterator()
	for iter.Next(ctx) {
		v := iter.Val()
		key := strings.TrimPrefix(v, r.prefix)
		key = strings.TrimPrefix(key, ":")

		if re.MatchString(key) {
			toDelete = append(toDelete, v)
		}
	}

	return r.client.Del(ctx, toDelete...).Err()
}

func (r *RedisStore) SetString(ctx context.Context, id string, value string) error {
	return r.client.Set(ctx, r.prefixKey(id), value, 0).Err()
}

func (r *RedisStore) GetString(ctx context.Context, id string) (string, error) {
	return r.client.Get(ctx, r.prefixKey(id)).Result()
}

func (r *RedisStore) SetInt(ctx context.Context, id string, value int) error {
	return r.client.Set(ctx, r.prefixKey(id), value, 0).Err()
}

func (r *RedisStore) GetInt(ctx context.Context, id string) (int, error) {
	res, err := r.client.Get(ctx, r.prefixKey(id)).Result()
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(res)
}

// Increment will adjust an int by +1.
func (r *RedisStore) Increment(ctx context.Context, id string) error {
	return r.client.Incr(ctx, r.prefixKey(id)).Err()
}

// Decrement will adjust an int by -1.
func (r *RedisStore) Decrement(ctx context.Context, id string) error {
	return r.client.Decr(ctx, r.prefixKey(id)).Err()
}

// IncrementBy will adjust an int by +value.
func (r *RedisStore) IncrementBy(ctx context.Context, id string, value int) error {
	return r.client.IncrBy(ctx, r.prefixKey(id), int64(value)).Err()
}

// DecrementBy will adjust an int by -value.
func (r *RedisStore) DecrementBy(ctx context.Context, id string, value int) error {
	return r.client.DecrBy(ctx, r.prefixKey(id), int64(value)).Err()
}

func (r *RedisStore) SetMap(ctx context.Context, id string, value map[string]string) error {
	vals := make([]string, 0)

	for k, v := range value {
		vals = append(vals, k)
		vals = append(vals, v)
	}

	return r.client.HSet(ctx, r.prefixKey(id), vals).Err()
}

func (r *RedisStore) GetMap(ctx context.Context, id string) (map[string]string, error) {
	return r.client.HGetAll(ctx, r.prefixKey(id)).Result()
}

func (r *RedisStore) SetMapValue(ctx context.Context, id string, key string, value string) error {
	return r.client.HSet(ctx, r.prefixKey(id), key, value).Err()
}

func (r *RedisStore) GetMapValue(ctx context.Context, id string, key string) (string, error) {
	return r.client.HGet(ctx, r.prefixKey(id), key).Result()
}

func (r *RedisStore) SetObject(ctx context.Context, id string, obj interface{}) error {
	bytes, err := r.objSer(obj)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, r.prefixKey(id), string(bytes), 0).Err()
}

func (r *RedisStore) GetObject(ctx context.Context, id string, dest interface{}) error {
	stored, err := r.client.Get(ctx, r.prefixKey(id)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return NoSuchItem
		}
		return err
	}
	return r.objDes([]byte(stored), dest)
}

func (r *RedisStore) ListSize(ctx context.Context, id string) (int, error) {
	return r.QueueSize(ctx, id)
}

func (r *RedisStore) ListSet(ctx context.Context, id string, value []string) error {
	// Delete any existing items before setting the list
	pipe := r.client.TxPipeline()
	pipe.Del(ctx, r.prefixKey(id))

	// Cannot use []string as []interface{}
	var values []interface{}
	for _, v := range value {
		values = append(values, v)
	}

	pipe.RPush(ctx, r.prefixKey(id), values...)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStore) ListGet(ctx context.Context, id string) ([]string, error) {
	return r.client.LRange(ctx, r.prefixKey(id), 0, -1).Result()
}

func (r *RedisStore) ListGetAt(ctx context.Context, id string, pos int) (string, error) {
	return r.client.LIndex(ctx, r.prefixKey(id), int64(pos)).Result()
}

func (r *RedisStore) ListSetAt(ctx context.Context, id string, pos int, value interface{}) error {
	return r.client.LSet(ctx, r.prefixKey(id), int64(pos), value).Err()
}

func (r *RedisStore) ListInsert(ctx context.Context, id string, pos int, value interface{}) error {
	pivot, err := r.client.LIndex(ctx, r.prefixKey(id), int64(pos)).Result()
	if err != nil {
		return err
	}
	return r.client.LInsertBefore(ctx, r.prefixKey(id), pivot, value).Err()
}

func (r *RedisStore) ListDeleteAt(ctx context.Context, id string, pos int) error {
	pivot, err := r.client.LIndex(ctx, r.prefixKey(id), int64(pos)).Result()
	if err != nil {
		return err
	}
	return r.client.LRem(ctx, r.prefixKey(id), 1, pivot).Err()
}

func (r *RedisStore) ListDeleteElem(ctx context.Context, id string, elem interface{}) error {
	return r.client.LRem(ctx, r.prefixKey(id), 0, elem).Err()
}

func (r *RedisStore) QueueSize(ctx context.Context, id string) (int, error) {
	cnt, err := r.client.LLen(ctx, r.prefixKey(id)).Result()
	return int(cnt), err
}

func (r *RedisStore) QueuePush(ctx context.Context, id string, value string) error {
	return r.QueuePushHead(ctx, id, value)
}

func (r *RedisStore) QueuePushHead(ctx context.Context, id string, value string) error {
	return r.client.LPush(ctx, r.prefixKey(id), value).Err()
}

func (r *RedisStore) QueuePushTail(ctx context.Context, id string, value string) error {
	return r.client.RPush(ctx, r.prefixKey(id), value).Err()
}

func (r *RedisStore) QueuePop(ctx context.Context, id string) (string, error) {
	return r.QueuePopTail(ctx, id)
}

func (r *RedisStore) QueuePopTail(ctx context.Context, id string) (string, error) {
	rv, err := r.client.RPop(ctx, r.prefixKey(id)).Result()

	if errors.Is(err, redis.Nil) {
		return "", NoSuchItem
	}

	return rv, err
}

func (r *RedisStore) QueuePopHead(ctx context.Context, id string) (string, error) {
	rv, err := r.client.LPop(ctx, r.prefixKey(id)).Result()

	if errors.Is(err, redis.Nil) {
		return "", NoSuchItem
	}

	return rv, err
}

func (r *RedisStore) QueueBlockingTransfer(ctx context.Context, srcID, dstID string) (string, error) {
	// TODO: update cmd to BLMOVE when go-redis version is upgraded
	// https://redis.io/commands/blmove
	return r.client.BRPopLPush(ctx, r.prefixKey(srcID), r.prefixKey(dstID), 0).Result()
}

func (r *RedisStore) SetExpires(ctx context.Context, id string, duration time.Duration) error {
	return r.client.Expire(ctx, id, duration).Err()
}

func (r *RedisStore) SetExpiresAt(ctx context.Context, id string, when time.Time) error {
	return r.client.ExpireAt(ctx, id, when).Err()
}

func (r *RedisStore) RemoveExpiration(ctx context.Context, id string) error {
	return r.client.Persist(ctx, id).Err()
}

func (r *RedisStore) GetTTL(ctx context.Context, id string) (time.Duration, error) {
	return r.client.TTL(ctx, id).Result()
}
