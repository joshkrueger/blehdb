package blehdb

import "fmt"

// Config provides the necessary configuration to the BlehDB server.
type Config struct {
	// StorageDir specifies the base path for persisting data on disk. The server
	// will attempt to create this path if it does not already exist.
	StorageDir string

	// RaftBind specifies the bind address for the Raft server.
	RaftBind string

	// RPCBind specifies the bind address for the RPC server.
	RPCBind string
}

func DefaultConfig() *Config {
	return &Config{
		RaftBind: ":11000",
		RPCBind:  ":12000",
	}
}

func ValidateConfig(config *Config) error {
	if config.StorageDir == "" {
		return fmt.Errorf("A StorageDir must be specified")
	}

	return nil
}
