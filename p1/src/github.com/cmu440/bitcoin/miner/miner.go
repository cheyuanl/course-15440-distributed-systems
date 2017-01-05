package main

import (
	"fmt"
	"os"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	client, err := lsp.NewClient(hostport, lsp.NewParams())
	joinMessage := &bitcoin.QueryWithMessage{0, *bitcoin.NewJoin()}
	err = bitcoin.SendMessage(client, joinMessage)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func workOnJob(client lsp.Client, queryId int, data string, lower uint64, upper uint64) {
	nonce, minHash := lower, bitcoin.Hash(data, lower)
	for i := lower + 1; i <= upper; i++ {
		hash := bitcoin.Hash(data, i)
		if hash < minHash {
			minHash = hash
			nonce = i
		}
	}

	resultMessage := bitcoin.NewResult(minHash, nonce)
	resultMessage.Lower = lower
	resultMessage.Upper = upper
	resultQueryWithMessage := &bitcoin.QueryWithMessage{queryId, *resultMessage}
	bitcoin.SendMessage(client, resultQueryWithMessage)
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	for {
		queryWithMessage, err := bitcoin.GetMessage(miner)
		if err == nil {
			queryId, message := queryWithMessage.QueryId, queryWithMessage.Message

			switch message.Type {
			case bitcoin.Request:
				data, lower, upper := message.Data, message.Lower, message.Upper
				go workOnJob(miner, queryId, data, lower, upper)
			default:
			}
		}
	}
}
