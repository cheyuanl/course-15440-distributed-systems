// Contains the implementation of a LSP server.

package lsp

import "errors"
import "strconv"
import "github.com/cmu440/lspnet"
import "encoding/json"
import "fmt"

type server struct {
	addr           *lspnet.UDPAddr
	clients        map[int]*ClientInfo
	newConnections chan *lspnet.UDPConn
}

type ClientInfo struct {
	connection *lspnet.UDPConn
	buffer     map[int][]byte
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

	srv := &server{
		addr,
		make(map[int]*ClientInfo),
		make(chan *lspnet.UDPConn),
	}

	go acceptClients(srv)
	go runEventLoop(srv)

	return srv, nil
}

func (srv *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return -1, nil, errors.New("not yet implemented")
}

func (srv *server) Write(connID int, payload []byte) error {
	return errors.New("not yet implemented")
}

func (srv *server) CloseConn(connID int) error {
	return errors.New("not yet implemented")
}

func (srv *server) Close() error {
	return errors.New("not yet implemented")
}

func acceptClients(srv *server) {
	for {
		conn, err := lspnet.ListenUDP("udp", srv.addr)
		if err == nil {
			// new client
			srv.newConnections <- conn
		}
	}
}

func runEventLoop(srv *server) {
	clientId := 1

	for {
		select {
		case connection := <-srv.newConnections:
			client := &ClientInfo{connection, make(map[int][]byte), make(chan int)}
			srv.clients[clientId] = client
			clientId += 1
			go readHandlerForClient(srv, client)
			go writeHandlerForClient(srv, client)
		}
	}
}

func readHandlerForClient(srv *server, client *ClientInfo) {
	for {
		select {
		case <-client.quitSignal:
			return
		default:
			var packetInByte []byte = make([]byte, 2000)
			n, err := client.connection.Read(packetInByte)
			if err == nil {
				packetInByte = packetInByte[0:n]
				if err == nil {
					var packet Message
					err = json.Unmarshal(packetInByte, &packet)
					if err == nil {
						fmt.Printf("Message: %v\n", packet)
					}
				}
			}
		}
	}
}

func writeHandlerForClient(srv *server, client *ClientInfo) {
}
