package blehdb

type Management struct {
	server *Server
}

type JoinRequest struct {
	Address string
}

func (m *Management) Join(args *JoinRequest, reply *string) error {
	f := m.server.raft.AddPeer(args.Address)

	if f.Error() != nil {
		return f.Error()
	}

	*reply = "Peer Added"

	return nil
}
