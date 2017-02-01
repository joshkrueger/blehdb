package blehdb

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/hashicorp/raft"
	"github.com/joshkrueger/blehdb/store"
)

type messageType uint8

const (
	CreateBucketRequestType messageType = iota
	DeleteBucketRequestType
	SetItemRequestType
	DeleteItemRequestType
)

func encodeMessage(t messageType, c *command) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(uint8(t))

	err := json.NewEncoder(&buf).Encode(c)
	return buf.Bytes(), err
}

func decodeMessage(msg []byte, c interface{}) error {
	return json.NewDecoder(bytes.NewReader(msg)).Decode(c)
}

type command struct {
	Bucket string
	Key    string
	Value  string
}

type blehFSM struct {
	logger *log.Logger
	store  *store.BlehStore
}

func NewFSM() (*blehFSM, error) {
	fsm := &blehFSM{
		logger: log.New(os.Stdout, "[FSM] ", log.LstdFlags),
		store:  store.New(),
	}

	return fsm, nil
}

func (b *blehFSM) Apply(log *raft.Log) interface{} {
	buf := log.Data

	msgType := messageType(buf[0])

	switch msgType {
	case CreateBucketRequestType:
		return b.applyCreateBucket(buf[1:], log.Index)
	case DeleteBucketRequestType:
		return b.applyDeleteBucket(buf[1:], log.Index)
	case SetItemRequestType:
		return b.applySetItem(buf[1:], log.Index)
	case DeleteItemRequestType:
		return b.applyDeleteItem(buf[1:], log.Index)
	default:
		b.logger.Printf("WARNING: ignoring unknown message type (%d)", msgType)
		return nil
	}
}

func (b *blehFSM) Store() *store.BlehStore {
	return b.store
}

func (b *blehFSM) applySetItem(buf []byte, index uint64) interface{} {
	var c command
	err := decodeMessage(buf, &c)
	if err != nil {
		return err
	}
	b.logger.Printf("(Index:%v) Setting Key: '%s' on Bucket: '%s' to the value '%s'", index, c.Key, c.Bucket, c.Value)
	err = b.store.SetItem(c.Bucket, c.Key, c.Value)
	if err != nil {
		b.logger.Printf("error during set: %v", err)
	}
	return err
}

func (b *blehFSM) applyDeleteItem(buf []byte, index uint64) interface{} {
	var c command
	err := decodeMessage(buf, &c)
	if err != nil {
		return err
	}
	b.logger.Printf("(Index:%v) Deleting Key: '%s' on Bucket: '%s'", index, c.Key, c.Bucket)
	err = b.store.DeleteItem(c.Bucket, c.Key)
	if err != nil {
		b.logger.Printf("error during set: %v", err)
	}
	return err
}

func (b *blehFSM) applyCreateBucket(buf []byte, index uint64) interface{} {
	var c command
	err := decodeMessage(buf, &c)
	if err != nil {
		return err
	}
	b.logger.Printf("(Index:%v) Creating Bucket: '%s'", index, c.Bucket)
	err = b.store.CreateBucket(c.Bucket)
	if err != nil {
		b.logger.Printf("error during bucket creation: %v", err)
	}
	return err
}

func (b *blehFSM) applyDeleteBucket(buf []byte, index uint64) interface{} {
	var c command
	err := decodeMessage(buf, &c)
	if err != nil {
		return err
	}
	b.logger.Printf("(Index:%v) Deleting Bucket: '%s'", index, c.Bucket)
	err = b.store.DeleteBucket(c.Bucket)
	if err != nil {
		b.logger.Printf("error during bucket deletion: %v", err)
	}
	return err
}

func (b *blehFSM) Snapshot() (raft.FSMSnapshot, error) {
	b.logger.Println("Calling Snapshot")
	buf, err := b.store.Backup()
	return &fsmSnapshot{
		snap: buf,
	}, err
}

func (b *blehFSM) Restore(old io.ReadCloser) error {
	new, err := store.Restore(old)
	if err != nil {
		return err
	}
	b.store = new
	return nil
}

type fsmSnapshot struct {
	snap []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {

		if _, err := sink.Write(s.snap); err != nil {
			return err
		}

		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (s *fsmSnapshot) Release() {}
