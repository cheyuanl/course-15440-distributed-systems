// Contains the implementation of a LSP server.

package lsp

import "errors"
import "strconv"
import "github.com/cmu440/lspnet"

type server struct {
	addr    *lspnet.UDPAddr
	clients map[int]*ClientInfo
}

type ClientInfo struct {
	connection *lspnet.UDPConn
	quitSignal chan int
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}

	s := &server{
		addr,
		make(map[int]UDPConn),
	}

	go acceptClients(s)

	return s, nil
}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return -1, nil, errors.New("not yet implemented")
}

func (s *server) Write(connID int, payload []byte) error {
	return errors.New("not yet implemented")
}

func (s *server) CloseConn(connID int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}

func acceptClients(s *server) {
	id := 1
	for {
		conn, err := lspnet.ListenUDP("udp", s.addr)
		if err == nil {
			// get new client
			c := &ClientInfo{conn, make(chan int)}
			s.clients[id] = c
			id += 1
		}
	}
}
