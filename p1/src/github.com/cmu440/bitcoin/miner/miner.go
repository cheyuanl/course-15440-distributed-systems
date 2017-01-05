package main

import (
	"fmt"
	"log"
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

	err := bitcoin.SendMessage(client, resultQueryWithMessage)
	if err != nil {
		fmt.Printf("[Miner %v] Error: %v\n", client.ConnID(), err)
	} else {
		fmt.Printf("[Miner %v] Send Result Back to Server: %v, %v\n", client.ConnID(), minHash, nonce)
	}
}

func main() {
	const (
		name = "log_miner.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)

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
		queryWithMessage, err := bitcoin.GetMessage(miner)
		if err == nil {
			queryId, message := queryWithMessage.QueryId, queryWithMessage.Message

			switch message.Type {
			case bitcoin.Request:
				data, lower, upper := message.Data, message.Lower, message.Upper
				LOGF.Printf("Get Request from Server: %v, %v, %v\n", data, lower, upper)
				go workOnJob(miner, queryId, data, lower, upper)
			default:
			}
		} else {
			LOGF.Printf("[Miner %v] Error: %v\n", miner.ConnID(), err)
			break
		}
	}
}
