package store

import (
	"bytes"
	"io/ioutil"
	"testing"
)

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
		t.Errorf("CreateBucket returned error: %v", err)
	}

	err = s.CreateBucket("foo")
	if err == nil {
		t.Errorf("CreateBucket should return error when creating a bucket that already exists")
	}
}

func stringSliceContains(s []string, obj string) bool {
	for _, v := range s {
		if v == obj {
			return true
		}
	}
	return false
}

func TestListBuckets(t *testing.T) {
	s := New()
	s.CreateBucket("foo")
	s.CreateBucket("bar")
	s.CreateBucket("baz")

	buckets := s.ListBuckets()
	expectedBuckets := []string{"foo", "bar", "baz"}
	if len(buckets) != len(expectedBuckets) {
		t.Error("The number of buckets does not match the expected value")
	}

	for _, v := range expectedBuckets {
		if !stringSliceContains(buckets, v) {
			t.Errorf("bucket '%v' is missing. got: %v", v, buckets)
		}
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

	_, err = s.GetItem("dne", "foo")
	if err == nil {
		t.Error("GetItem on non-existent bucket should return an error")
	}

	err = s.DeleteItem("dne", "foo")
	if err == nil {
		t.Error("DeleteItem on non-existent bucket should return an error")
	}
}

func TestBackup(t *testing.T) {
	s := New()
	s.CreateBucket("foo")
	s.SetItem("foo", "bar", "baz")
	s.SetItem("foo", "foo", "bar")

	b, err := s.Backup()
	if err != nil {
		t.Error("backup should not have returned an error")
	}

	buff := bytes.NewBuffer(b)

	ss, err := Restore(ioutil.NopCloser(buff))
	if err != nil {
		t.Error("unexpected error in restore")
	}

	v, err := ss.GetItem("foo", "bar")
	if err != nil {
		t.Error("GetItem should not have returned an error")
	}

	if v != "baz" {
		t.Error("Expected value of restored DB does not match actual")
	}
}
