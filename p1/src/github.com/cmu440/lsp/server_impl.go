// Contains the implementation of a LSP server.

package lsp

import "errors"
import "strconv"
import "github.com/cmu440/lspnet"
import "fmt"

type server struct {
	addr       *lspnet.UDPAddr
	clients    map[int]*ClientInfo
	connection *lspnet.UDPConn
	dataBuffer chan *DataBufferElement
}

type ClientInfo struct {
	connectionId       int
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
	conn, err := lspnet.ListenUDP("udp", addr)

	s := &server{
		addr,
		make(map[int]*ClientInfo),
		conn,
		make(chan *DataBufferElement),
	}

	go runEventLoop(s)

	return s, nil
}

func (s *server) Read() (int, []byte, error) {
	element := <-s.dataBuffer
	return element.connectionId, element.data, nil
}

func (s *server) Write(connID int, payload []byte) error {
	client := s.clients[connID]
	message := NewData(connID, client.nextSequenceNumber, len(payload), payload)
	go WriteMessage(s.connection, client.addr, message)
	client.nextSequenceNumber += 1

	return nil
}

func (s *server) CloseConn(connID int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}

func runEventLoop(s *server) {
	connectionId := 1

	for {
		select {
		default:
			request, clientAddr, err := ReadMessage(s.connection)
			if err == nil {
				switch request.Type {
				case MsgConnect:
					fmt.Printf("New Connection From Client %v with ConnectionId %v\n", clientAddr, connectionId)

					// create new client info
					client := &ClientInfo{connectionId, clientAddr, 1}
					s.clients[connectionId] = client
					connectionId += 1

					// send ack
					response := NewAck(client.connectionId, 0)
					err = WriteMessage(s.connection, clientAddr, response)
					if err != nil {
						fmt.Printf("Server Error: %v\n", err)
					}
				case MsgData:
					fmt.Printf("New Data From Client: %v!\n", clientAddr)

					client := s.clients[request.ConnID]

					// save data into buffer
					s.dataBuffer <- &DataBufferElement{client.connectionId, request.Payload}

					// send ack
					response := NewAck(client.connectionId, request.SeqNum)
					err = WriteMessage(s.connection, clientAddr, response)
					if err != nil {
						fmt.Printf("Server Error: %v\n", err)
					}
				}
			}
		}
	}
}
