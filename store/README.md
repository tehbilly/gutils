## Store

This package implements storage functionality, allowing the persistence/retrieval of different data types to various
backends with a common API.

For usage examples review [storage_test.go](storage.go).

### Implementations

- In-memory _(non-persistent)_ storage in [memory.go](memory.go)
- [Redis](https://redis.io) storage implementation in [redis.go](redis.go)

### Future implementation ideas

- [memcached](https://memcached.org/)
- SQL database
- Local filesystem _(perhaps using something like [bbolt](https://github.com/etcd-io/bbolt))_
