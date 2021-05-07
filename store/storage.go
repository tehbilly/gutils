package store

import (
	"context"
	"errors"
	"time"
)

var (
	// NoSuchItem is returned by retrieval methods when an attempt is made to retrieve data that does not exist
	NoSuchItem = errors.New("no such item")
)

type Storage interface {
	// Exists checks for the existence of any value located at id.
	Exists(ctx context.Context, id string) bool
	// Delete removes any data associated with id.
	Delete(ctx context.Context, id string) error
	// DeleteAll will remove _all_ values in the backing storage managed by this store.
	DeleteAll(ctx context.Context) error
	// DeleteMatching will remove all key/value pairs where the key matches the provided pattern.
	DeleteMatching(ctx context.Context, pattern string) error
	// SetExpires will set data stored at id to expire after duration. If duration is negative the value will be
	// immediately deleted.
	SetExpires(ctx context.Context, id string, duration time.Duration) error
	// SetExpiresAt will set data stored at id to expire at a specific time. If time is in the past the value will be
	// immediately deleted.
	SetExpiresAt(ctx context.Context, id string, when time.Time) error
	// RemoveExpiration will set data stored at id to no longer expire.
	RemoveExpiration(ctx context.Context, id string) error
	// GetTTL will get the time.Duration left to live for data stored at id.
	GetTTL(ctx context.Context, id string) (time.Duration, error)

	// SetString creates or updates the value associated with id.
	SetString(ctx context.Context, id string, value string) error
	// GetString gets the current value of a string at id, if present.
	GetString(ctx context.Context, id string) (string, error)
	// SetInt creates or updates the value associated with id.
	SetInt(ctx context.Context, id string, value int) error
	// GetInt gets the current value of an int at id, if present.
	GetInt(ctx context.Context, id string) (int, error)

	// Increment will adjust an int by +1.
	Increment(ctx context.Context, id string) error
	// Decrement will adjust an int by -1.
	Decrement(ctx context.Context, id string) error
	// IncrementBy will adjust an int by +value.
	IncrementBy(ctx context.Context, id string, value int) error
	// DecrementBy will adjust an int by -value.
	DecrementBy(ctx context.Context, id string, value int) error

	// SetMap will create or update a map in storage at id.
	SetMap(ctx context.Context, id string, value map[string]string) error
	// GetMap will retrieve an entire map from storage at id.
	GetMap(ctx context.Context, id string) (map[string]string, error)
	// SetMapValue will update an existing map at id, creating or replacing the value associated with key.
	SetMapValue(ctx context.Context, id string, key string, value string) error
	// GetMapValue will retrieve a single value an existing map in storage at id.
	GetMapValue(ctx context.Context, id string, key string) (string, error)

	// SetObject will create or update a stored object at id.
	SetObject(ctx context.Context, id string, obj interface{}) error
	// GetObject will return an object from storage at id, if it exists.
	GetObject(ctx context.Context, id string, dest interface{}) error

	// ListSize will return the number of items in a list stored at id.
	ListSize(ctx context.Context, id string) (int, error)
	// ListSet will remove any existing items currently stored at id, and will push all items from value into the
	// cleared list. If no list exists, one will be created.
	ListSet(ctx context.Context, id string, value []string) error
	// ListGet will retrieve all values from a list stored at id.
	ListGet(ctx context.Context, id string) ([]string, error)
	// ListGetAt will return the list element at pos. An error will be returned if the list does not exist or the pos is
	// out of range.
	ListGetAt(ctx context.Context, id string, pos int) (string, error)
	// ListSetAt will set the list element at pos to value. An error will be returned if the list does not exist or the
	// pos is out of range.
	ListSetAt(ctx context.Context, id string, pos int, value interface{}) error
	// ListInsert will insert a new value at pos, shifting other list elements towards the end of the list.
	ListInsert(ctx context.Context, id string, pos int, value interface{}) error
	// ListDeleteAt removes an item from the list at pos.
	ListDeleteAt(ctx context.Context, id string, pos int) error
	// ListDeleteElem deletes all members of `id` that match `elem`
	ListDeleteElem(ctx context.Context, id string, elem interface{}) error

	// QueueSize will return the number of items in a queue stored at id.
	QueueSize(ctx context.Context, id string) (int, error)
	// QueuePush will push an item to the head of a queue.
	QueuePush(ctx context.Context, id string, value string) error
	// QueuePushHead will push an item to the head of a queue.
	QueuePushHead(ctx context.Context, id string, value string) error
	// QueuePushTail will push an item to the tail of a queue.
	QueuePushTail(ctx context.Context, id string, value string) error
	// QueuePop will pop an item off of the tail of the queue, removing it from the queue and returning the value.
	QueuePop(ctx context.Context, id string) (string, error)
	// QueuePopTail will pop an item off of the tail of the queue, removing it from the queue and returning the value.
	QueuePopTail(ctx context.Context, id string) (string, error)
	// QueuePopHead will pop an item off of the head of the queue, removing it from the queue and returning the value.
	QueuePopHead(ctx context.Context, id string) (string, error)
	// QueueBlockingTransfer blocks until an element transfers from src to dst.
	QueueBlockingTransfer(ctx context.Context, srcID, dstID string) (string, error)
}

// ObjectSerializer is used to marshal objects for storage
type ObjectSerializer func(value interface{}) ([]byte, error)

// ObjectDeserializer is used to unmarshal objects from storage
type ObjectDeserializer func(data []byte, target interface{}) error
