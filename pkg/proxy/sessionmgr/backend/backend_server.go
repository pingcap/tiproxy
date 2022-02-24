package backend

type BackendServer struct {
	addr string
}

func (server *BackendServer) Addr() string {
	return server.addr
}
