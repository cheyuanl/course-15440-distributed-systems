package bitcoin

import (
	"encoding/json"

	"github.com/cmu440/lsp"
)

type QueryWithMessage struct {
	QueryId int
	Message Message
}

func GetMessage(client lsp.Client) (*QueryWithMessage, error) {
	payload, err := client.Read()
	if err == nil {
		var message QueryWithMessage
		err = json.Unmarshal(payload, &message)
		if err == nil {
			return &message, err
		}
	}

	return nil, err
}

func SendMessage(client lsp.Client, message *QueryWithMessage) error {
	var packet []byte
	packet, err := json.Marshal(message)
	if err == nil {
		err = client.Write(packet)
	}

	return err
}
