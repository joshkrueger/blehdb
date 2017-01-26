package store

import "testing"

func TestNewStore(t *testing.T) {

	s := New()
	if s == nil {
		t.Error("New() should not have returned nil")
	}
}

func TestCreateBucket(t *testing.T) {
	s := New()
	var err error
	err = s.CreateBucket("foo")
	if err != nil {
		t.Error("CreateBucket should not have returned nil")
	}
}

func TestDuplicateBucketError(t *testing.T) {
	s := New()
	var err error
	s.CreateBucket("foo")

	err = s.CreateBucket("foo")
	if err == nil {
		t.Error("Duplicate buckets should have returned an Error")
	}
}

func TestBucketExists(t *testing.T) {
	s := New()

	if s.BucketExists("foo") {
		t.Error("bucket should not exist in a fresh store")
	}

	s.CreateBucket("foo")
	if !s.BucketExists("foo") {
		t.Error("created bucket should exist")
	}
}

func TestBucketDelete(t *testing.T) {
	s := New()
	s.CreateBucket("foo")
	s.CreateBucket("bar")

	err := s.DeleteBucket("foo")
	if err != nil {
		t.Errorf("unexpected error when deleting bucket: %v", err)
	}

	if !s.BucketExists("bar") {
		t.Error("unrelated bucket should not have been deleted")
	}
}

func TestSetItem(t *testing.T) {
	var err error
	s := New()
	s.CreateBucket("foo")

	err = s.SetItem("foo", "bar", "baz")
	if err != nil {
		t.Errorf("unexpected error when setting item: %v", err)
	}

	err = s.SetItem("notfoo", "bar", "baz")
	if err == nil {
		t.Errorf("expected error when setting item on non-existant bucket was not returned")
	}
}

func TestItem(t *testing.T) {
	var err error
	s := New()
	s.CreateBucket("foo")
	s.SetItem("foo", "bar", "baz")

	v, err := s.GetItem("foo", "bar")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if v != "baz" {
		t.Errorf("expected value to be 'baz', got: '%v'", v)
	}

	err = s.DeleteItem("foo", "bar")
	if err != nil {
		t.Errorf("unexpected error when deleting key: %v", err)
	}

	_, err = s.GetItem("foo", "bar")
	if err == nil {
		t.Error("item should not exist anymore")
	}
}
