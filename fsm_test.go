package blehdb

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/hashicorp/raft"
)

func randString(l int) string {
	buf := make([]byte, l)
	for i := 0; i < (l+1)/2; i++ {
		buf[i] = byte(rand.Intn(256))
	}
	return fmt.Sprintf("%x", buf)[:l]
}

func BenchmarkEncodeMessage(b *testing.B) {
	c := &command{
		Bucket: randString(32),
		Key:    randString(16),
		Value:  randString(512),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encodeMessage(SetItemRequestType, c)
	}
}

func BenchmarkDecodeMessage(b *testing.B) {
	c := &command{
		Bucket: randString(32),
		Key:    randString(16),
		Value:  randString(512),
	}

	msg, _ := encodeMessage(SetItemRequestType, c)
	decode := make([]byte, len(msg)-1)
	copy(decode, msg[1:])
	var com command
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decodeMessage(decode, &com)
	}
}

func TestEncodeDecodeMessage(t *testing.T) {
	c := &command{
		Bucket: randString(32),
		Key:    randString(16),
		Value:  randString(512),
	}

	var err error
	msg, err := encodeMessage(SetItemRequestType, c)
	if err != nil {
		t.Error("expected encode error to not be nil")
	}

	var com command
	err = decodeMessage(msg[1:], &com)
	if err != nil {
		t.Error("expected decode error to not be nil")
	}

	if com != *c {
		t.Error("expected objects to match")
	}
}

func mockLog(buf []byte) *raft.Log {
	return &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  buf,
	}
}

func setupFSM(t *testing.T) *blehFSM {
	fsm, err := NewFSM()
	if err != nil {
		t.Fatalf("error creating FSM: %v", err)
	}

	return fsm
}

func TestApplySetItem(t *testing.T) {
	fsm := setupFSM(t)

	fsm.Store().CreateBucket("foo")

	setComm := &command{
		Bucket: "foo",
		Key:    "bar",
		Value:  "baz",
	}

	msg, err := encodeMessage(SetItemRequestType, setComm)
	if err != nil {
		t.Fatalf("error encoding message: %v", err)
	}

	resp := fsm.Apply(mockLog(msg))
	if resp != nil {
		t.Fatalf("error applying raft log: %v", err)
	}

	val, err := fsm.Store().GetItem("foo", "bar")
	if err != nil {
		t.Fatalf("error fetching item: %v", err)
	}

	if val != "baz" {
		t.Fatalf("value shold be: 'baz', got: '%v'", val)
	}
}

func TestApplySetItem_badBucket(t *testing.T) {
	fsm := setupFSM(t)

	setComm := &command{
		Bucket: "foo",
		Key:    "bar",
		Value:  "baz",
	}

	msg, err := encodeMessage(SetItemRequestType, setComm)
	if err != nil {
		t.Fatalf("error encoding message: %v", err)
	}

	resp := fsm.Apply(mockLog(msg))
	if resp == nil {
		t.Fatalf("non-existent bucket should have raised an error")
	}
}

func TestApplyCreateBucket(t *testing.T) {
	fsm := setupFSM(t)

	createComm := &command{
		Bucket: "foo",
	}

	msg, err := encodeMessage(CreateBucketRequestType, createComm)
	if err != nil {
		t.Fatalf("error encoding message: %v", err)
	}

	resp := fsm.Apply(mockLog(msg))
	if resp != nil {
		t.Fatalf("error applying raft log: %v", err)
	}
}

func TestApplyDeleteItem(t *testing.T) {
	fsm := setupFSM(t)

	fsm.Store().CreateBucket("foo")
	fsm.Store().SetItem("foo", "bar", "baz")

	delComm := &command{
		Bucket: "foo",
		Key:    "bar",
	}

	msg, err := encodeMessage(DeleteItemRequestType, delComm)
	if err != nil {
		t.Fatalf("error encoding message: %v", err)
	}

	resp := fsm.Apply(mockLog(msg))
	if resp != nil {
		t.Fatalf("error applying raft log: %v", resp)
	}

	val, err := fsm.Store().GetItem("foo", "bar")
	if err == nil {
		t.Errorf("Item should have been deleted. An error should have been returned. Got: %v", err)
		t.Errorf("Got value: %v", val)
	}
}

func TestApplyDeleteBucket(t *testing.T) {
	fsm := setupFSM(t)

	fsm.Store().CreateBucket("foo")

	delComm := &command{
		Bucket: "foo",
	}

	msg, err := encodeMessage(DeleteBucketRequestType, delComm)
	if err != nil {
		t.Fatalf("error encoding message: %v", err)
	}

	resp := fsm.Apply(mockLog(msg))
	if resp != nil {
		t.Fatalf("error applying raft log: %v", resp)
	}

	if fsm.Store().BucketExists("foo") {
		t.Errorf("bucket 'foo' should not exist")
	}

	resp = fsm.Apply(mockLog(msg))
	if resp != nil {
		t.Fatalf("deleting non-existent bucket should not error: %v", resp)
	}
}

func TestApplyUnknown(t *testing.T) {
	buf := []byte("badcommand")

	fsm := setupFSM(t)
	resp := fsm.Apply(mockLog(buf))

	if resp != nil {
		t.Errorf("unexpected error, unknown commands should be skipped")
	}
}
