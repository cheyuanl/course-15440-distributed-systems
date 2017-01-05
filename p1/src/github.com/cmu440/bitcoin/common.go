package bitcoin

import (
	"encoding/json"

	"github.com/cmu440/lsp"
)

type QueryWithMessage struct {
	QueryId int
	Message Message
}

func GetMessage(client lsp.Client) (*Message, error) {
	payload, err := client.Read()
	if err == nil {
		var message Message
		err = json.Unmarshal(payload, &message)
		if err == nil {
			return &message, err
		}
	}

	return nil, err
}

func SendMessage(client lsp.Client, message interface{}) error {
	var packet []byte
	packet, err := json.Marshal(message)
	if err == nil {
		err = client.Write(packet)
	}

	return err
}
