// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

type ServerStatus int

const (
	NOT_CLOSEING ServerStatus = iota
	START_CLOSING
	CLIENTS_CLOSED
	CONNECTION_CLOSED
)

type server struct {
	addr             *lspnet.UDPAddr
	clients          map[int]*ClientInfo
	connection       *lspnet.UDPConn
	inMessagesChan   chan *MessageAndAddr
	dataBufferChan   chan *DataBufferElement
	status           ServerStatus
	clientClosedChan chan int
}

type MessageAndAddr struct {
	message *Message
	addr    *lspnet.UDPAddr
}

type ClientInfo struct {
	connectionId             int
	addr                     *lspnet.UDPAddr
	inMessageChan            chan *Message
	outMessages              map[int]*Message
	outMessageChan           chan *Message
	outMessageSequenceNumber int
	epochSignal              chan int
	receivedData             bool
	dataBuffer               map[int][]byte
	dataBufferSequenceNumber int
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
	if err != nil {
		return nil, err
	}

	s := &server{
		addr,
		make(map[int]*ClientInfo),
		conn,
		make(chan *MessageAndAddr, 1000),
		make(chan *DataBufferElement, 1000),
		NOT_CLOSEING,
		make(chan int, 1000)}

	go readHandlerForServer(s)
	go eventLoopForServer(s, params)

	return s, nil
}

func (s *server) Read() (int, []byte, error) {
	element := <-s.dataBufferChan
	return element.connectionId, element.data, nil
}

func (s *server) Write(connID int, payload []byte) error {
	// fmt.Printf("[Server] Write Data to Client %v\n", connID)

	client := s.clients[connID]
	message := NewData(connID, -1, len(payload), payload)
	client.outMessageChan <- message

	return nil
}

func (s *server) CloseConn(connID int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	s.status = START_CLOSING

	for {
		if s.status == CLIENTS_CLOSED {
			s.connection.Close()
		}
		if s.status == CONNECTION_CLOSED {
			fmt.Printf("[Server] Server Closed!\n")
			return nil
		}
		time.Sleep(time.Second)
	}
}

func readHandlerForServer(s *server) {
	for {
		inMessage, clientAddr, err := ReadMessage(s.connection)
		if err != nil {
			if s.status == CLIENTS_CLOSED {
				s.status = CONNECTION_CLOSED
				fmt.Printf("[Server] Connection Closed!\n")
				return
			}
		} else {
			s.inMessagesChan <- &MessageAndAddr{inMessage, clientAddr}
		}
	}
}

func eventLoopForServer(s *server, params *Params) {
	connectionId := 1
	timer := time.NewTimer(time.Duration(params.EpochMillis) * time.Millisecond)

	for {
		select {
		case messageAndAddr := <-s.inMessagesChan:
			inMessage := messageAndAddr.message
			clientAddr := messageAndAddr.addr
			// fmt.Printf("Client Request: %v\n", inMessage)

			switch inMessage.Type {
			case MsgConnect:
				// fmt.Printf("New Connection From Client %v\n", connectionId)

				// create new client info
				client := &ClientInfo{
					connectionId,
					clientAddr,
					make(chan *Message, 1000),
					make(map[int]*Message),
					make(chan *Message, 1000),
					1,
					make(chan int, 1000),
					false,
					make(map[int][]byte),
					1}
				s.clients[connectionId] = client
				connectionId += 1
				go writeHandlerForClient(s, client, params)

				// send ack
				// fmt.Printf("New Connection Ack to Client %v\n", connectionId-1)
				response := NewAck(client.connectionId, 0)
				go WriteMessage(s.connection, clientAddr, response)
			default:
				client := s.clients[inMessage.ConnID]
				client.inMessageChan <- inMessage
			}

		case connectionId := <-s.clientClosedChan:
			fmt.Printf("[Server] Client %v Closed!\n", connectionId)

			delete(s.clients, connectionId)
			if len(s.clients) == 0 {
				fmt.Printf("[Server] All Clients Closed!\n")
				s.status = CLIENTS_CLOSED
				return
			}
		case <-timer.C:
			// fmt.Printf("[Server] Epoch!\n")
			for _, client := range s.clients {
				client.epochSignal <- 1
			}

			timer.Reset(time.Duration(params.EpochMillis) * time.Millisecond)
		}
	}
}

func writeHandlerForClient(s *server, c *ClientInfo, params *Params) {
	for {
		if s.status == START_CLOSING && len(c.outMessages) == 0 && len(c.dataBuffer) == 0 {
			s.clientClosedChan <- c.connectionId
			return
		}

		minUnAckedOutMessageSequenceNumber := c.outMessageSequenceNumber
		for sequenceNumber, _ := range c.outMessages {
			if minUnAckedOutMessageSequenceNumber > sequenceNumber {
				minUnAckedOutMessageSequenceNumber = sequenceNumber
			}
		}

		select {
		case inMessage := <-c.inMessageChan:
			switch inMessage.Type {
			case MsgData:
				// fmt.Printf("New Data From Client %v with Seq %v!\n", c.connectionId, inMessage.SeqNum)

				// save data into buffer
				_, exists := c.dataBuffer[inMessage.SeqNum]
				if !exists {
					c.dataBuffer[inMessage.SeqNum] = inMessage.Payload
				}

				if inMessage.SeqNum == c.dataBufferSequenceNumber {
					i := c.dataBufferSequenceNumber
					for {
						data, exists := c.dataBuffer[i]
						if exists {
							s.dataBufferChan <- &DataBufferElement{c.connectionId, data}
							c.dataBufferSequenceNumber += 1
							delete(c.dataBuffer, i)
						} else {
							break
						}
						i += 1
					}
				}

				// send ack
				response := NewAck(c.connectionId, inMessage.SeqNum)
				go WriteMessage(s.connection, c.addr, response)

			case MsgAck:
				// fmt.Printf("New Ack From Client: %v!\n", c.connectionId)

				_, exists := c.outMessages[inMessage.SeqNum]
				if exists {
					delete(c.outMessages, inMessage.SeqNum)
				}
			}

		case <-c.epochSignal:
			// fmt.Printf("[Server-Client %v] Epoch!\n", c.connectionId)

			if c.dataBufferSequenceNumber == 1 && len(c.dataBuffer) == 0 {
				outMessage := NewAck(c.connectionId, 0)
				go WriteMessage(s.connection, c.addr, outMessage)
			}
			for _, outMessage := range c.outMessages {
				// fmt.Printf("[Server-Client %v] Resend Message from Server: %v %v\n", c.connectionId, outMessage.SeqNum, minUnAckedOutMessageSequenceNumber)
				go WriteMessage(s.connection, c.addr, outMessage)
			}

		default:
			time.Sleep(time.Nanosecond)

			if c.outMessageSequenceNumber-minUnAckedOutMessageSequenceNumber < params.WindowSize {
				select {
				case outMessage := <-c.outMessageChan:
					outMessage.SeqNum = c.outMessageSequenceNumber
					// fmt.Printf("Server-Client Write Message: %v %v\n", outMessage, minUnAckedOutMessageSequenceNumber)

					c.outMessages[c.outMessageSequenceNumber] = outMessage
					c.outMessageSequenceNumber += 1
					go WriteMessage(s.connection, c.addr, outMessage)

				default:
				}
			}
		}
	}
}
