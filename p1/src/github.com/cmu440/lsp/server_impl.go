// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

type server struct {
	addr       *lspnet.UDPAddr
	clients    map[int]*ClientInfo
	connection *lspnet.UDPConn
	inMessages chan *MessageAndAddr
	dataBuffer chan *DataBufferElement
}

type MessageAndAddr struct {
	message *Message
	addr    *lspnet.UDPAddr
}

type ClientInfo struct {
	connectionId       int
	addr               *lspnet.UDPAddr
	outMessages        map[int]*Message
	outMessagesChan    chan *Message
	nextSequenceNumber int
	epochSignal        chan int
	receivedData       bool
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
		make(chan *MessageAndAddr),
		make(chan *DataBufferElement)}

	go readHandlerForServer(s)
	go eventLoopForServer(s, params)

	return s, nil
}

func (s *server) Read() (int, []byte, error) {
	element := <-s.dataBuffer
	return element.connectionId, element.data, nil
}

func (s *server) Write(connID int, payload []byte) error {
	fmt.Printf("Write Data to Client %v\n", connID)

	client := s.clients[connID]
	message := NewData(connID, -1, len(payload), payload)
	client.outMessagesChan <- message

	return nil
}

func (s *server) CloseConn(connID int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}

func readHandlerForServer(s *server) {
	for {
		inMessage, clientAddr, err := ReadMessage(s.connection)
		if err != nil {
			fmt.Printf("Server Error: %v\n", err)
		} else {
			s.inMessages <- &MessageAndAddr{inMessage, clientAddr}
		}
	}
}

func eventLoopForServer(s *server, params *Params) {
	connectionId := 1
	timer := time.NewTimer(time.Duration(params.EpochMillis) * time.Millisecond)

	for {
		select {
		case <-timer.C:
			for _, client := range s.clients {
				client.epochSignal <- 1
			}
		case messageAndAddr := <-s.inMessages:
			inMessage := messageAndAddr.message
			clientAddr := messageAndAddr.addr
			fmt.Printf("Client Request: %v\n", inMessage)
			switch inMessage.Type {
			case MsgConnect:
				fmt.Printf("New Connection From Client %v\n", connectionId)

				// create new client info
				client := &ClientInfo{
					connectionId,
					clientAddr,
					make(map[int]*Message),
					make(chan *Message),
					1,
					make(chan int),
					false}
				s.clients[connectionId] = client
				connectionId += 1
				go writeHandlerForClient(s, client)

				// send ack
				response := NewAck(client.connectionId, 0)
				go WriteMessage(s.connection, clientAddr, response)
			case MsgData:
				fmt.Printf("New Data From Client: %v!\n", clientAddr)

				client := s.clients[inMessage.ConnID]
				client.receivedData = true

				// save data into buffer
				s.dataBuffer <- &DataBufferElement{client.connectionId, inMessage.Payload}

				// send ack
				response := NewAck(client.connectionId, inMessage.SeqNum)
				go WriteMessage(s.connection, clientAddr, response)
			}
		}
	}
}

func writeHandlerForClient(s *server, c *ClientInfo) {
	for {
		select {
		case <-c.epochSignal:
			if !c.receivedData {
				response := NewAck(c.connectionId, 0)
				go WriteMessage(s.connection, c.addr, response)
			} else {
				for _, outMessage := range c.outMessages {
					go WriteMessage(s.connection, c.addr, outMessage)
				}
			}
		case outMessage := <-c.outMessagesChan:
			c.outMessages[c.nextSequenceNumber] = outMessage
			c.nextSequenceNumber += 1
			err := WriteMessage(s.connection, c.addr, outMessage)
			if err != nil {
				fmt.Printf("Server Error: %v\n", err)
			}
		}
	}
}
