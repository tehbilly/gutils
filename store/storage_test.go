package store_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/suite"
	"github.com/tehbilly/gutils/store"
)

type storageSuite struct {
	suite.Suite

	mr     *miniredis.Miniredis
	stores map[string]store.Storage
}

func (s *storageSuite) SetupSuite() {
	mr, err := miniredis.Run()
	if err != nil {
		s.Fail("Unable to start miniredis")
	}
	s.mr = mr
	redisStore, _ := store.NewRedisStore("scox:test", &store.RedisOptions{Addr: s.mr.Addr()})
	s.stores = map[string]store.Storage{
		"redis":  redisStore,
		"memory": store.NewMemoryStore(),
	}
}

func (s *storageSuite) SetupTest() {
	// Ensure we have a clean slate for each test
	s.mr.FlushAll()
	s.stores["memory"] = store.NewMemoryStore()
}

func (s *storageSuite) TestExpiration() {
	for name, storage := range s.stores {
		s.Run(name, func() {
			s.Nil(storage.SetString(context.Background(), "exp:str", "value"))

			s.Nil(storage.SetExpires(context.Background(), "exp:str", 10*time.Second))
			ttl, err := storage.GetTTL(context.Background(), "exp:str")
			s.Nil(err)
			s.NotEqual(0, ttl) // TODO: Revisit tests if expiration use cases are found

			s.Nil(storage.SetExpiresAt(context.Background(), "exp:str", time.Now().Add(30*time.Second)))
			ttl, err = storage.GetTTL(context.Background(), "exp:str")
			s.Nil(err)
			s.NotEqual(0, ttl)

			s.Nil(storage.RemoveExpiration(context.Background(), "exp:str"))

			ttl, err = storage.GetTTL(context.Background(), "exp:str")
			s.Nil(err)
			s.Equal(time.Duration(0), ttl.Round(time.Second))
		})
	}
}

func (s *storageSuite) TestQueue() {
	for name, storage := range s.stores {
		s.Run(name, func() {
			// Make sure pop operations return "", NoSuchItem
			rv, err := storage.QueuePop(context.Background(), "nope")
			s.Equal("", rv)
			s.Equal(store.NoSuchItem, err)
			rv, err = storage.QueuePopHead(context.Background(), "nope")
			s.Equal("", rv)
			s.Equal(store.NoSuchItem, err)

			// Ensure we're working with an empty list
			size, err := storage.QueueSize(context.Background(), "queue")
			s.Nil(err)
			s.Equal(0, size)

			values := []string{"two", "three", "four", "five"}
			for _, v := range values {
				s.Nil(storage.QueuePush(context.Background(), "queue", v))
			}

			// We forgot one! Push to the tail
			s.Nil(storage.QueuePushTail(context.Background(), "queue", "one"))

			// Ensure QueueSize works
			size, err = storage.QueueSize(context.Background(), "queue")
			s.Nil(err)
			s.Equal(5, size)

			one, err := storage.QueuePop(context.Background(), "queue")
			s.Nil(err)
			s.Equal("one", one)

			five, err := storage.QueuePopHead(context.Background(), "queue")
			s.Nil(err)
			s.Equal("five", five)

			two, err := storage.QueuePop(context.Background(), "queue")
			s.Nil(err)
			s.Equal("two", two)

			three, err := storage.QueuePop(context.Background(), "queue")
			s.Nil(err)
			s.Equal("three", three)
		})
	}
}

func (s *storageSuite) TestObject() {
	for name, storage := range s.stores {
		s.Run(name, func() {
			type testObject struct {
				ID    string
				Name  string
				Value int
			}

			input := &testObject{
				ID:    "test-id",
				Name:  "Testy McTesterson",
				Value: 42,
			}

			s.Nil(storage.SetObject(context.Background(), "obj", input))

			// Assert we get can our object back out
			var output testObject
			s.Nil(storage.GetObject(context.Background(), "obj", &output))
			s.Equal(input, &output)

			// Assert that we get NoSuchItem trying to fetch a non-existent object
			s.Equal(store.NoSuchItem, storage.GetObject(context.Background(), "nope", &output))
		})
	}
}

func (s *storageSuite) TestList() {
	for name, storage := range s.stores {
		s.Run(name, func() {
			input := []string{"one", "two", "three"}

			s.NoError(storage.ListSet(context.Background(), "list", input))
			if name == "redis" {
				// Check that we have the list we think we will
				s.mr.CheckList(s.T(), "scox:test:list", "one", "two", "three")
			}

			// Ensure that setting a list doesn't simply append the items to an existing list
			s.NoError(storage.ListSet(context.Background(), "list", input))
			if name == "redis" {
				// Check that we have the list we think we will
				s.mr.CheckList(s.T(), "scox:test:list", "one", "two", "three")
			}

			output, err := storage.ListGet(context.Background(), "list")
			s.NoError(err)

			s.Equal(len(input), len(output))
			s.ElementsMatch(input, output)

			// Ensure we get the expected items at each position
			for i, expected := range input {
				v, err := storage.ListGetAt(context.Background(), "list", i)
				s.NoError(err)
				s.Equal(expected, v)
			}

			// Change the second element only
			s.NoError(storage.ListSetAt(context.Background(), "list", 1, "seven"))

			// Ensure we get the expected items at each position, except for the one we changed
			for i, iv := range input {
				expected := iv
				if i == 1 {
					expected = "seven"
				}
				v, err := storage.ListGetAt(context.Background(), "list", i)
				s.NoError(err)
				s.Equal(expected, v)
			}

			// Ensure the list's size is measured accurately
			ls, err := storage.ListSize(context.Background(), "list")
			s.NoError(err)
			s.EqualValues(3, ls)

			// Ensure we can insert a new item into the list, shifting existing elements
			s.NoError(storage.ListInsert(context.Background(), "list", 1, "eight"))
			check, err := storage.ListGet(context.Background(), "list")
			s.NoError(err)
			s.Equal([]string{"one", "eight", "seven", "three"}, check)

			// Ensure the list's size is measured accurately
			ls, err = storage.ListSize(context.Background(), "list")
			s.NoError(err)
			s.EqualValues(4, ls)

			s.NoError(storage.ListDeleteAt(context.Background(), "list", 1))
			check, err = storage.ListGet(context.Background(), "list")
			s.NoError(err)
			s.Equal([]string{"one", "seven", "three"}, check)
		})
	}
}

func (s *storageSuite) TestMapValue() {
	for name, storage := range s.stores {
		s.Run(name, func() {
			sourceMap := map[string]string{
				"foo": "bar",
				"baz": "quux",
				"int": "123",
			}

			s.Nil(storage.SetMap(context.Background(), "map-vals", sourceMap))
			s.Nil(storage.SetMapValue(context.Background(), "map-vals", "new", "val"))

			fullMap, err := storage.GetMap(context.Background(), "map-vals")
			s.Nil(err)

			s.Equal(map[string]string{"foo": "bar", "baz": "quux", "int": "123", "new": "val"}, fullMap)

			newVal, err := storage.GetMapValue(context.Background(), "map-vals", "new")
			s.Nil(err)

			s.Equal("val", newVal)
		})
	}
}

func (s *storageSuite) TestMap() {
	for name, storage := range s.stores {
		s.Run(name, func() {
			sourceMap := map[string]string{
				"foo": "bar",
				"baz": "quux",
			}

			s.Nil(storage.SetMap(context.Background(), "map", sourceMap))

			gotMap, err := storage.GetMap(context.Background(), "map")
			s.Nil(err)

			s.Equal(map[string]string{"foo": "bar", "baz": "quux"}, gotMap)
		})
	}
}

func (s *storageSuite) TestInt() {
	for name, storage := range s.stores {
		s.Run(name, func() {
			s.Nil(storage.SetInt(context.Background(), "int", 123))

			i, err := storage.GetInt(context.Background(), "int")
			s.Nil(err)
			s.Equal(i, 123)
		})
	}
}

func (s *storageSuite) TestString() {
	for name, storage := range s.stores {
		s.Run(name, func() {
			s.Nil(storage.SetString(context.Background(), "foo", "bar"))

			foo, err := storage.GetString(context.Background(), "foo")
			s.NoError(err)
			s.Equal("bar", foo)
		})
	}
}

func (s *storageSuite) TestExists() {
	for name, storage := range s.stores {
		s.Run(name, func() {
			s.Nil(storage.SetString(context.Background(), "exists", "bar"))
			s.True(storage.Exists(context.Background(), "exists"))
		})
	}
}

func (s *storageSuite) TestDelete() {
	for name, storage := range s.stores {
		s.Run(name, func() {
			// Standard single-key delete
			s.False(storage.Exists(context.Background(), "exists"))
			s.Nil(storage.SetString(context.Background(), "exists", "bar"))
			s.True(storage.Exists(context.Background(), "exists"))
			s.Nil(storage.Delete(context.Background(), "exists"))
			s.False(storage.Exists(context.Background(), "exists"))

			// Key/Value pairs used for multi-delete methods
			source := map[string]string{
				"one:foo": "foo",
				"one:bar": "bar",
				"two:foo": "foo",
				"two:bar": "bar",
			}

			// Ensure the keys don't exist
			for key := range source {
				s.False(storage.Exists(context.Background(), key), "Expected key not to exist: %s", key)
			}

			// Add our keys and ensure they exist
			for key, val := range source {
				s.Nil(storage.SetString(context.Background(), key, val))
				s.True(storage.Exists(context.Background(), key), "Expected key to exist: %s", key)
			}

			// Delete our first set
			s.Nil(storage.DeleteMatching(context.Background(), "^one"))
			for key := range source {
				if strings.HasPrefix(key, "one") {
					s.False(storage.Exists(context.Background(), key))
				} else {
					s.True(storage.Exists(context.Background(), key))
				}
			}

			// Delete our second set
			s.Nil(storage.DeleteMatching(context.Background(), "^two.*"))
			for key := range source {
				s.False(storage.Exists(context.Background(), key))
			}

			// Add our keys and ensure they exist
			for key, val := range source {
				s.Nil(storage.SetString(context.Background(), key, val))
				s.True(storage.Exists(context.Background(), key), "Expected key to exist: %s", key)
			}

			// Delete all and ensure all of our keys are gone
			s.Nil(storage.DeleteAll(context.Background()))
			for key := range source {
				s.False(storage.Exists(context.Background(), key))
			}
		})
	}
}

func TestStorage(t *testing.T) {
	suite.Run(t, new(storageSuite))
}

func (s *storageSuite) TestQueueBlockingTransfer() {
	for name, storage := range s.stores {
		s.Run(name, func() {
			go func() {
				time.Sleep(100 * time.Millisecond)
				s.Nil(storage.QueuePush(context.Background(), "src", "msg"))
			}()

			ctx, cnFn := context.WithDeadline(context.Background(), time.Now().Add(500*time.Millisecond))
			defer cnFn()
			msg, err := storage.QueueBlockingTransfer(ctx, "src", "dst")
			s.Nil(err)
			s.Equal("msg", msg)
		})
	}
}

func (s *storageSuite) TestListDeleteElem() {
	for name, storage := range s.stores {
		s.Run(name, func() {
			s.Nil(storage.ListSet(context.Background(), name, []string{"a", "b", "b", "c", "a", "d", "e", "f", "a"}))
			s.Nil(storage.ListDeleteElem(context.Background(), name, "a"))
			list, err := storage.ListGet(context.Background(), name)
			s.Nil(err)
			s.Equal([]string{"b", "b", "c", "d", "e", "f"}, list)
		})
	}
}
