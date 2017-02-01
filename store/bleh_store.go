package store

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

type BlehStore struct {
	Buckets    map[string]*bucket
	bucketLock sync.RWMutex
}

type bucket struct {
	Items    map[string]*item
	itemLock sync.RWMutex
}

type item struct {
	Value string
}

func New() *BlehStore {
	return &BlehStore{
		Buckets: make(map[string]*bucket),
	}
}

func (b *BlehStore) Backup() ([]byte, error) {
	b.bucketLock.Lock()
	defer b.bucketLock.Unlock()

	buf, err := json.Marshal(b)
	return buf, err
}

func Restore(rc io.ReadCloser) (*BlehStore, error) {
	bs := New()

	err := json.NewDecoder(rc).Decode(&bs)
	return bs, err
}

func (b *BlehStore) ListBuckets() []string {
	b.bucketLock.RLock()
	defer b.bucketLock.RUnlock()

	var keys []string
	for k, _ := range b.Buckets {
		keys = append(keys, k)
	}

	return keys
}

func (b *BlehStore) BucketExists(name string) bool {
	b.bucketLock.RLock()
	defer b.bucketLock.RUnlock()
	_, ok := b.Buckets[name]

	return ok
}

func (b *BlehStore) CreateBucket(name string) error {
	b.bucketLock.Lock()
	defer b.bucketLock.Unlock()

	if _, ok := b.Buckets[name]; ok {
		return fmt.Errorf("Bucket '%s' already exists", name)
	}

	b.Buckets[name] = newBucket()

	return nil
}

func (b *BlehStore) DeleteBucket(name string) error {
	b.bucketLock.Lock()
	defer b.bucketLock.Unlock()
	delete(b.Buckets, name)
	if _, ok := b.Buckets[name]; ok {
		return fmt.Errorf("Bucket '%s' did not delete", name)
	}

	return nil
}

func (b *BlehStore) SetItem(bucket, key, value string) error {
	b.bucketLock.RLock()
	defer b.bucketLock.RUnlock()

	bb, ok := b.Buckets[bucket]
	if !ok {
		return fmt.Errorf("bucket '%s' does not exist", bucket)
	}

	err := bb.Set(key, value)
	return err
}

func (b *BlehStore) GetItem(bucket, key string) (string, error) {
	b.bucketLock.RLock()
	defer b.bucketLock.RUnlock()

	bb, ok := b.Buckets[bucket]
	if !ok {
		return "", fmt.Errorf("bucket '%s' does not exist", bucket)
	}

	return bb.Get(key)
}

func newBucket() *bucket {
	return &bucket{
		Items: make(map[string]*item),
	}
}

func (b *BlehStore) DeleteItem(bucket, key string) error {
	b.bucketLock.RLock()
	defer b.bucketLock.RUnlock()

	bb, ok := b.Buckets[bucket]
	if !ok {
		return fmt.Errorf("bucket '%s' does not exist", bucket)
	}

	return bb.Delete(key)
}

func (b *bucket) Set(key, value string) error {
	b.itemLock.Lock()
	defer b.itemLock.Unlock()

	b.Items[key] = &item{
		Value: value,
	}

	return nil
}

func (b *bucket) Get(key string) (string, error) {
	b.itemLock.RLock()
	defer b.itemLock.RUnlock()

	i, ok := b.Items[key]
	if !ok {
		return "", fmt.Errorf("Key '%v' not found", key)
	}

	return i.Value, nil
}

func (b *bucket) Delete(key string) error {
	b.itemLock.Lock()
	defer b.itemLock.Unlock()

	delete(b.Items, key)

	return nil
}
