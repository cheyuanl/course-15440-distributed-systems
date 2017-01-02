// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

type client struct {
	connectionId       int
	connection         *lspnet.UDPConn
	inMessages         chan *Message
	outMessages        map[int]*Message
	nextSequenceNumber int
	outMessagesChan    chan *Message
	dataBuffer         chan []byte
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	serverAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}

	// get connection
	connection, err := lspnet.DialUDP("udp", nil, serverAddr)
	if err != nil {
		return nil, err
	}

	c := &client{
		-1,
		connection,
		make(chan *Message),
		make(map[int]*Message),
		0,
		make(chan *Message),
		make(chan []byte)}
	statusChan := make(chan int)

	// send connection message
	connectMessage := NewConnect()
	c.outMessages[c.nextSequenceNumber] = connectMessage
	c.nextSequenceNumber += 1
	WriteMessage(connection, nil, connectMessage)

	go readHandlerForClient(c)
	go eventLoopForClient(c, statusChan, params)

	status := <-statusChan

	if status == 0 {
		return c, nil
	}

	return c, errors.New("Can not create new client!")
}

func (c *client) ConnID() int {
	return c.connectionId
}

func (c *client) Read() ([]byte, error) {
	data := <-c.dataBuffer
	return data, nil
}

func (c *client) Write(payload []byte) error {
	fmt.Printf("Write Data to Server\n")

	message := NewData(c.connectionId, -1, len(payload), payload)
	c.outMessagesChan <- message

	return nil
}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}

func readHandlerForClient(c *client) {
	for {
		inMessage, _, err := ReadMessage(c.connection)
		if err != nil {
			fmt.Printf("Client Error: %v\n", err)
		} else {
			c.inMessages <- inMessage
		}
	}
}

func eventLoopForClient(c *client, statusChan chan int, params *Params) {
	epochCount := 0
	timer := time.NewTimer(time.Duration(params.EpochMillis) * time.Millisecond)

	for {
		select {
		case <-timer.C:
			epochCount += 1
			if epochCount == params.EpochLimit {
				statusChan <- 1
				return
			} else {
			}
		case inMessage := <-c.inMessages:
			fmt.Printf("Client Request: %v\n", inMessage)
			switch inMessage.Type {
			case MsgData:
				fmt.Printf("New Data From Server!\n")

				// save data into buffer
				c.dataBuffer <- inMessage.Payload

				// send ack
				response := NewAck(c.connectionId, inMessage.SeqNum)
				go WriteMessage(c.connection, nil, response)
			case MsgAck:
				outMessage, exists := c.outMessages[inMessage.SeqNum]
				if exists {
					if outMessage.Type == MsgConnect {
						c.connectionId = inMessage.ConnID
						statusChan <- 0
					}
					delete(c.outMessages, inMessage.SeqNum)
				}
			}
		case outMessage := <-c.outMessagesChan:
			c.outMessages[c.nextSequenceNumber] = outMessage
			c.nextSequenceNumber += 1
			go WriteMessage(c.connection, nil, outMessage)
		}
	}
}
