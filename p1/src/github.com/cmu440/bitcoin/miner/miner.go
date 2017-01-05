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
	err = bitcoin.SendMessage(client, *bitcoin.NewJoin())
	if err != nil {
		return nil, err
	}

	return client, nil
}

func workOnJob(client lsp.Client, data string, lower uint64, upper uint64) {
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

	err := bitcoin.SendMessage(client, resultMessage)
	if err != nil {
		fmt.Printf("[Miner %v] Error: %v\n", client.ConnID(), err)
	} else {
		fmt.Printf("[Miner %v] Send Result Back to Server: %v, %v\n", client.ConnID(), minHash, nonce)
	}
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
	} else {
		fmt.Println("Join Server!")
	}

	defer miner.Close()

	for {
		message, err := bitcoin.GetMessage(miner)
		if err == nil {
			switch message.Type {
			case bitcoin.Request:
				data, lower, upper := message.Data, message.Lower, message.Upper
				LOGF.Printf("Get Request from Server: %v, %v, %v\n", data, lower, upper)
				go workOnJob(miner, data, lower, upper)
			default:
			}
		} else {
			LOGF.Printf("[Miner %v] Error: %v\n", miner.ConnID(), err)
			break
		}
	}
}
