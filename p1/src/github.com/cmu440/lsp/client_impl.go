// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

type client struct {
	connectionId             int
	connection               *lspnet.UDPConn
	inMessages               chan *Message
	outMessages              map[int]*Message
	outMessageChan           chan *Message
	outMessageSequenceNumber int
	dataBuffer               map[int][]byte
	dataBufferChan           chan []byte
	dataBufferSequenceNumber int
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
		make(chan *Message, 1000),
		make(map[int]*Message),
		make(chan *Message, 1000),
		0,
		make(map[int][]byte),
		make(chan []byte),
		1}
	statusSignal := make(chan int)

	// send connect message
	fmt.Printf("Send Connect Message\n")
	connectMessage := NewConnect()
	c.outMessages[c.outMessageSequenceNumber] = connectMessage
	c.outMessageSequenceNumber += 1
	WriteMessage(connection, nil, connectMessage)

	go readHandlerForClient(c)
	go eventLoopForClient(c, statusSignal, params)

	status := <-statusSignal

	if status == 0 {
		return c, nil
	}

	return c, errors.New("Can Not Create New Client!")
}

func (c *client) ConnID() int {
	return c.connectionId
}

func (c *client) Read() ([]byte, error) {
	data := <-c.dataBufferChan
	return data, nil
}

func (c *client) Write(payload []byte) error {
	// fmt.Printf("Write Data to Server\n")

	message := NewData(c.connectionId, -1, len(payload), payload)
	c.outMessageChan <- message

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

func eventLoopForClient(c *client, statusSignal chan int, params *Params) {
	epochCount := 0
	timer := time.NewTimer(time.Duration(params.EpochMillis) * time.Millisecond)

	for {
		minUnAckedOutMessageSequenceNumber := c.outMessageSequenceNumber
		for sequenceNumber, _ := range c.outMessages {
			if minUnAckedOutMessageSequenceNumber > sequenceNumber {
				minUnAckedOutMessageSequenceNumber = sequenceNumber
			}
		}

		// fmt.Printf("[Client %v] Min-UnAcked %v, Next %v\n", c.connectionId, minUnAckedOutMessageSequenceNumber, c.outMessageSequenceNumber)
		// fmt.Printf("[Client %v] Next Buffer %v\n", c.connectionId, c.dataBufferSequenceNumber)

		if c.outMessageSequenceNumber-minUnAckedOutMessageSequenceNumber < params.WindowSize {
			select {
			case inMessage := <-c.inMessages:
				// fmt.Printf("[Client %v] Server Request: %v\n", c.connectionId, inMessage)
				epochCount = 0

				switch inMessage.Type {
				case MsgData:
					// fmt.Printf("[Client %v] New Data From Server: %v %v!\n", c.connectionId, inMessage.SeqNum, c.dataBufferSequenceNumber)

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
								c.dataBufferChan <- data
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
					go WriteMessage(c.connection, nil, response)

				case MsgAck:
					// fmt.Printf("Ack From Server!\n")

					outMessage, exists := c.outMessages[inMessage.SeqNum]
					if exists {
						if outMessage.Type == MsgConnect {
							c.connectionId = inMessage.ConnID
							statusSignal <- 0
						}
						delete(c.outMessages, inMessage.SeqNum)
					}
				}

			case outMessage := <-c.outMessageChan:
				outMessage.SeqNum = c.outMessageSequenceNumber
				// fmt.Printf("[Client %v] Write Message: %v\n", c.connectionId, outMessage)

				c.outMessages[c.outMessageSequenceNumber] = outMessage
				c.outMessageSequenceNumber += 1
				go WriteMessage(c.connection, nil, outMessage)

			case <-timer.C:
				// fmt.Printf("[Client %v] Epoch!\n", c.connectionId)

				epochCount += 1

				if c.connectionId < 0 {
					connectMessage := NewConnect()
					go WriteMessage(c.connection, nil, connectMessage)
				} else {
					if c.dataBufferSequenceNumber == 1 && len(c.dataBuffer) == 0 {
						outMessage := NewAck(c.connectionId, 0)
						go WriteMessage(c.connection, nil, outMessage)
					}
				}
				for _, outMessage := range c.outMessages {
					// fmt.Printf("[Client %v] Resend Message from Client: %v\n", c.connectionId, outMessage)
					go WriteMessage(c.connection, nil, outMessage)
				}

				timer.Reset(0)

			}
		} else {
			select {
			case inMessage := <-c.inMessages:
				// fmt.Printf("[Client %v] Server Request: %v\n", c.connectionId, inMessage)
				epochCount = 0

				switch inMessage.Type {
				case MsgData:
					// fmt.Printf("[Client %v] New Data From Server: %v %v!\n", c.connectionId, inMessage.SeqNum, c.dataBufferSequenceNumber)

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
								c.dataBufferChan <- data
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
					go WriteMessage(c.connection, nil, response)

				case MsgAck:
					// fmt.Printf("Ack From Server!\n")

					outMessage, exists := c.outMessages[inMessage.SeqNum]
					if exists {
						if outMessage.Type == MsgConnect {
							c.connectionId = inMessage.ConnID
							statusSignal <- 0
						}
						delete(c.outMessages, inMessage.SeqNum)
					}
				}

			case <-timer.C:
				// fmt.Printf("[Client %v] Epoch!\n", c.connectionId)

				epochCount += 1

				if c.connectionId < 0 {
					connectMessage := NewConnect()
					go WriteMessage(c.connection, nil, connectMessage)
				} else {
					if c.dataBufferSequenceNumber == 1 && len(c.dataBuffer) == 0 {
						outMessage := NewAck(c.connectionId, 0)
						go WriteMessage(c.connection, nil, outMessage)
					}
				}
				for _, outMessage := range c.outMessages {
					// fmt.Printf("[Client %v] Resend Message from Client: %v\n", c.connectionId, outMessage)
					go WriteMessage(c.connection, nil, outMessage)
				}

				timer.Reset(0)

			}
		}
	}
}
