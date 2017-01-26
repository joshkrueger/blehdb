package blehdb

import (
	"fmt"
	"math/rand"
	"testing"
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
