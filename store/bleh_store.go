package store

import (
	"fmt"
	"sync"
)

type BlehStore struct {
	buckets    map[string]*bucket
	bucketLock sync.RWMutex
}

type bucket struct {
	items    map[string]*item
	itemLock sync.RWMutex
}

type item struct {
	Value string
}

func New() *BlehStore {
	return &BlehStore{
		buckets: make(map[string]*bucket),
	}
}

func (b *BlehStore) BucketExists(name string) bool {
	b.bucketLock.RLock()
	defer b.bucketLock.RUnlock()
	_, ok := b.buckets[name]

	return ok
}

func (b *BlehStore) CreateBucket(name string) error {
	b.bucketLock.Lock()
	defer b.bucketLock.Unlock()

	if _, ok := b.buckets[name]; ok {
		return fmt.Errorf("Bucket '%s' already exists", name)
	}

	b.buckets[name] = newBucket()

	return nil
}

func (b *BlehStore) DeleteBucket(name string) error {
	b.bucketLock.Lock()
	defer b.bucketLock.Unlock()
	delete(b.buckets, name)
	if _, ok := b.buckets[name]; ok {
		return fmt.Errorf("Bucket '%s' did not delete", name)
	}

	return nil
}

func (b *BlehStore) SetItem(bucket, key, value string) error {
	b.bucketLock.RLock()
	defer b.bucketLock.RUnlock()

	bb, ok := b.buckets[bucket]
	if !ok {
		return fmt.Errorf("bucket '%s' does not exist", bucket)
	}

	err := bb.Set(key, value)
	return err
}

func (b *BlehStore) GetItem(bucket, key string) (string, error) {
	b.bucketLock.RLock()
	defer b.bucketLock.RUnlock()

	bb, ok := b.buckets[bucket]
	if !ok {
		return "", fmt.Errorf("bucket '%s' does not exist", bucket)
	}

	return bb.Get(key)
}

func newBucket() *bucket {
	return &bucket{
		items: make(map[string]*item),
	}
}

func (b *BlehStore) DeleteItem(bucket, key string) error {
	b.bucketLock.RLock()
	defer b.bucketLock.RUnlock()

	bb, ok := b.buckets[bucket]
	if !ok {
		return fmt.Errorf("bucket '%s' does not exist", bucket)
	}

	return bb.Delete(key)
}

func (b *bucket) Set(key, value string) error {
	b.itemLock.Lock()
	defer b.itemLock.Unlock()

	b.items[key] = &item{
		Value: value,
	}

	return nil
}

func (b *bucket) Get(key string) (string, error) {
	b.itemLock.RLock()
	defer b.itemLock.RUnlock()

	i, ok := b.items[key]
	if !ok {
		return "", fmt.Errorf("Key '%v' not found", key)
	}

	return i.Value, nil
}

func (b *bucket) Delete(key string) error {
	b.itemLock.Lock()
	defer b.itemLock.Unlock()

	delete(b.items, key)

	return nil
}
