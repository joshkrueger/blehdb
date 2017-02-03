package blehdb

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
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

	rpcListener net.Listener
	rpcServer   *rpc.Server

	manager *Management
}

func NewServer(config *Config) (*Server, error) {
	if err := ValidateConfig(config); err != nil {
		return nil, fmt.Errorf("Invalid Config: %v", err)
	}

	s := &Server{
		config:    config,
		logger:    log.New(os.Stdout, "[BLEHDB] ", log.LstdFlags),
		rpcServer: rpc.NewServer(),
	}

	if err := s.setupRaft(); err != nil {
		// TODO: shutdown cleanly
		return nil, fmt.Errorf("Failed to start Raft: %v", err)
	}

	if err := s.setupRPC(); err != nil {
		return nil, fmt.Errorf("Failed to start RPC: %v", err)
	}

	return s, nil
}

func (s *Server) setupRPC() error {
	s.manager = &Management{s}
	s.rpcServer.Register(s.manager)

	l, err := net.Listen("tcp", s.config.RPCBind)
	if err != nil {
		return err
	}

	s.rpcListener = l
	go s.listen()

	return nil
}

func (s *Server) listen() {
	s.rpcServer.Accept(s.rpcListener)
	s.logger.Println("RPC.Accept has returned. This might be an error.")
}

func (s *Server) Join(addr string) error {
	client, err := rpc.Dial("tcp", addr)

	if err != nil {
		return err
	}

	args := &JoinRequest{
		Address: s.config.RaftBind,
	}

	var resp string

	err = client.Call("Management.Join", args, &resp)
	if err != nil {
		return err
	}

	s.logger.Printf("Raft join returned: %v", resp)

	return nil
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
