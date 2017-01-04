package lsp

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/lspnet"
)

func ReadMessage(connection *lspnet.UDPConn) (*Message, *lspnet.UDPAddr, error) {
	packet := make([]byte, 2000)
	n, addr, err := connection.ReadFromUDP(packet)
	if err == nil {
		packet = packet[0:n]
		var message Message
		err = json.Unmarshal(packet, &message)
		if err == nil {
			return &message, addr, nil
		}
	}

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	return nil, addr, err
}

func WriteMessage(connection *lspnet.UDPConn, addr *lspnet.UDPAddr, message *Message) error {
	var packet []byte
	packet, err := json.Marshal(message)
	if err == nil {
		if addr != nil {
			_, err = connection.WriteToUDP(packet, addr)
		} else {
			_, err = connection.Write(packet)
		}
	}

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	return err
}

type Status int

const (
	NOT_CLOSING Status = iota
	START_CLOSING
	HANDLER_CLOSED
	CONNECTION_CLOSED
	CLOSED
)
