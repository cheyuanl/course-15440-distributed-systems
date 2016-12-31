// Contains the implementation of a LSP client.

package lsp

import "errors"
import "github.com/cmu440/lspnet"
import "fmt"

type client struct {
	connectionId       int
	connection         *lspnet.UDPConn
	nextSequenceNumber int
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

	// create connection
	connection, err := lspnet.DialUDP("udp", nil, serverAddr)
	if err != nil {
		return nil, err
	}
	request := NewConnect()
	WriteMessage(connection, nil, request)
	response, _, err := ReadMessage(connection)
	if err != nil {
		return nil, err
	}
	if response.Type != MsgAck {
		return nil, err
	}

	c := &client{response.ConnID, connection, 1, make(chan []byte)}

	go readHandler(c)

	return c, nil
}

func (c *client) ConnID() int {
	return c.connectionId
}

func (c *client) Read() ([]byte, error) {
	data := <-c.dataBuffer
	return data, nil
}

func (c *client) Write(payload []byte) error {
	message := NewData(c.connectionId, c.nextSequenceNumber, len(payload), payload)
	go WriteMessage(c.connection, nil, message)
	c.nextSequenceNumber += 1

	return nil
}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}

func readHandler(c *client) {
	for {
		request, _, err := ReadMessage(c.connection)
		if err != nil {
			fmt.Printf("Client Error: %v\n", err)
		}

		fmt.Printf("Request: %v\n", request)
		switch request.Type {
		case MsgData:
			fmt.Printf("New Data From Server!\n")
			c.dataBuffer <- request.Payload
			response := NewAck(c.connectionId, request.SeqNum)
			err = WriteMessage(c.connection, nil, response)
			if err != nil {
				fmt.Printf("Client Error: %v\n", err)
			}
		}
	}
}
