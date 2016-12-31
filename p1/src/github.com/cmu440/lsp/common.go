package lsp

import (
	"encoding/json"
	"github.com/cmu440/lspnet"
)

func ReadMessage(connection *lspnet.UDPConn) (Message, *lspnet.UDPAddr, error) {
	packet := make([]byte, 2000)
	n, addr, err := connection.ReadFromUDP(packet)
	if err == nil {
		packet = packet[0:n]
		if err == nil {
			var message Message
			err = json.Unmarshal(packet, &message)
			if err == nil {
				return message, addr, nil
			}
		}
	}
	return Message{}, addr, err
}

func WriteMessage(connection *lspnet.UDPConn, addr *lspnet.UDPAddr, message *Message) error {
	var packet []byte
	packet, err := json.Marshal(message)
	if addr != nil {
		_, err = connection.WriteToUDP(packet, addr)
	} else {
		_, err = connection.Write(packet)
	}
	return err
}
