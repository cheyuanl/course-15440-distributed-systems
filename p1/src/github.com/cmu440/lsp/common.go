package lsp

import (
	"encoding/json"
	"github.com/cmu440/lspnet"
)

func ReadMessage(connection *lspnet.UDPConn) (Message, error) {
	packet := make([]byte, 2000)
	n, err := connection.Read(packet)
	if err == nil {
		packet = packet[0:n]
		if err == nil {
			var message Message
			err = json.Unmarshal(packet, &message)
			if err == nil {
				return message, nil
			}
		}
	}
	return Message{}, err
}

func WriteMessage(connection *lspnet.UDPConn, message *Message) error {
	var packet []byte
	packet, err := json.Marshal(message)
	_, err = connection.Write(packet)
	return err
}
