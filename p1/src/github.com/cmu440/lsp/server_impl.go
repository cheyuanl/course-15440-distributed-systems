// Contains the implementation of a LSP server.

package lsp

import "errors"
import "strconv"
import "github.com/cmu440/lspnet"
import "fmt"

type server struct {
	addr           *lspnet.UDPAddr
	clients        map[int]*ClientInfo
	newConnections chan *lspnet.UDPConn
	dataBuffer     chan *DataBufferElement
}

type ClientInfo struct {
	connectionId       int
	connection         *lspnet.UDPConn
	addr               *lspnet.UDPAddr
	nextSequenceNumber int
}

type DataBufferElement struct {
	connectionId int
	data         []byte
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}

	srv := &server{
		addr,
		make(map[int]*ClientInfo),
		make(chan *lspnet.UDPConn),
		make(chan *DataBufferElement),
	}

	go acceptClients(srv)
	go runEventLoop(srv)

	return srv, nil
}

func (srv *server) Read() (int, []byte, error) {
	element := <-srv.dataBuffer
	return element.connectionId, element.data, nil
}

func (srv *server) Write(connID int, payload []byte) error {
	client := srv.clients[connID]
	message := NewData(connID, client.nextSequenceNumber, len(payload), payload)
	go WriteMessage(client.connection, client.addr, message)
	client.nextSequenceNumber += 1

	return nil
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
	connectionId := 1

	for {
		select {
		case connection := <-srv.newConnections:
			client := &ClientInfo{connectionId, connection, nil, 1}
			srv.clients[connectionId] = client
			connectionId += 1
			go readHandlerForClient(srv, client)
		}
	}
}

func readHandlerForClient(srv *server, client *ClientInfo) {
	for {
		select {
		default:
			request, clientAddr, err := ReadMessage(client.connection)
			if err == nil {
				switch request.Type {
				case MsgConnect:
					fmt.Printf("New Connection From Client: %v!\n", clientAddr)
					response := NewAck(client.connectionId, 0)
					err = WriteMessage(client.connection, clientAddr, response)
					if err != nil {
						fmt.Printf("Server Error: %v\n", err)
					}
					client.addr = clientAddr
				case MsgData:
					fmt.Printf("New Data From Client: %v!\n", clientAddr)
					srv.dataBuffer <- &DataBufferElement{client.connectionId, request.Payload}
					response := NewAck(client.connectionId, request.SeqNum)
					err = WriteMessage(client.connection, clientAddr, response)
					if err != nil {
						fmt.Printf("Server Error: %v\n", err)
					}
				}
			}
		}
	}
}
