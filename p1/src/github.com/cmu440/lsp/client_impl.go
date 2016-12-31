// Contains the implementation of a LSP client.

package lsp

import "errors"
import "encoding/json"
import "github.com/cmu440/lspnet"
import "fmt"

type client struct {
	serverAddr *lspnet.UDPAddr
	connection *lspnet.UDPConn
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
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}

	// create connection
	connection, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	packet := NewConnect()
	var packetInByte []byte
	packetInByte, err = json.Marshal(*packet)
	_, err = connection.Write(packetInByte)
	if err != nil {
		return nil, err
	}

	c := &client{addr, connection}

	return c, nil
}

func (c *client) ConnID() int {
	return -1
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return nil, errors.New("not yet implemented")
}

func (c *client) Write(payload []byte) error {
	return errors.New("not yet implemented")
}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}
