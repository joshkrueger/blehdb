package blehdb

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type Server struct {
	config    *Config
	logger    *log.Logger
	fsm       *blehFSM
	raft      *raft.Raft
	raftStore *raftboltdb.BoltStore
	peerStore *raft.JSONPeers
}

func NewServer(config *Config) (*Server, error) {
	if err := ValidateConfig(config); err != nil {
		return nil, fmt.Errorf("Invalid Config: %v", err)
	}

	s := &Server{
		config: config,
		logger: log.New(os.Stdout, "[BLEHDB] ", log.LstdFlags),
	}

	if err := s.setupRaft(); err != nil {
		// TODO: shutdown cleanly
		return nil, fmt.Errorf("Failed to start Raft: %v", err)
	}

	return s, nil
}

func (s *Server) setupRaft() error {
	var err error

	s.fsm, err = NewFSM()
	if err != nil {
		return err
	}

	config := raft.DefaultConfig()
	config.Logger = s.logger

	bootstrap := true
	if bootstrap {
		s.logger.Println("Entering bootstrap mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	addr, err := net.ResolveTCPAddr("tcp", s.config.RaftBind)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransportWithLogger(s.config.RaftBind, addr, 3, 10*time.Second, s.logger)
	if err != nil {
		return err
	}

	s.peerStore = raft.NewJSONPeers(s.config.StorageDir, transport)

	snapshots, err := raft.NewFileSnapshotStoreWithLogger(s.config.StorageDir, 2, s.logger)
	if err != nil {
		return err
	}

	s.raftStore, err = raftboltdb.NewBoltStore(filepath.Join(s.config.StorageDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new BoltStore: %v", err)
	}

	s.raft, err = raft.NewRaft(config, s.fsm, s.raftStore, s.raftStore, snapshots, s.peerStore, transport)
	if err != nil {
		return fmt.Errorf("new Raft: %v", err)
	}

	return nil
}

func (s *Server) Set(bucket, key, value string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("this member is not the leader, cannot mutate values")
	}

	c := &command{
		Bucket: bucket,
		Key:    key,
		Value:  value,
	}

	b, err := encodeMessage(SetItemRequestType, c)
	if err != nil {
		return err
	}

	return s.applyRaft(b)
}

func (s *Server) Get(bucket, key string) (string, error) {
	val, err := s.fsm.Store().GetItem(bucket, key)
	return val, err
}

func (s *Server) BucketExists(bucket string) bool {
	return s.fsm.Store().BucketExists(bucket)
}

func (s *Server) ListBuckets() []string {
	return s.fsm.Store().ListBuckets()
}

func (s *Server) Delete(bucket, key string) error {
	c := &command{
		Bucket: bucket,
		Key:    key,
	}

	b, err := encodeMessage(DeleteItemRequestType, c)
	if err != nil {
		return err
	}

	return s.applyRaft(b)
}

func (s *Server) CreateBucket(name string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("this member is not the leader, cannot mutate values")
	}

	c := &command{
		Bucket: name,
	}

	b, err := encodeMessage(CreateBucketRequestType, c)
	if err != nil {
		return err
	}

	return s.applyRaft(b)
}

func (s *Server) DeleteBucket(name string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("this member is not the leader, cannot mutate values")
	}

	c := &command{
		Bucket: name,
	}

	b, err := encodeMessage(DeleteBucketRequestType, c)
	if err != nil {
		return err
	}

	return s.applyRaft(b)
}

func (s *Server) applyRaft(msg []byte) error {
	f := s.raft.Apply(msg, 10*time.Second)
	if f.Error() != nil {
		return f.Error()
	}
	res := f.Response()
	if resErr, ok := res.(error); ok {
		return resErr
	}

	return nil
}
