package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

type server struct {
	lspServer          lsp.Server
	miners             []int
	queryMinHashResult map[int]uint64
	queryNonceResult   map[int]uint64
	queryCount         map[string]int
	queryClient        map[int]int
}

func startServer(port int) (*server, error) {
	lspServer, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}

	srv := &server{
		lspServer,
		make([]int, 0),
		make(map[int]uint64),
		make(map[int]uint64),
		make(map[string]int),
		make(map[int]int)}

	return srv, nil
}

func getMessage(srv *server) (int, *bitcoin.QueryWithMessage, error) {
	cid, payload, err := srv.lspServer.Read()
	if err == nil {
		var message bitcoin.QueryWithMessage
		err = json.Unmarshal(payload, &message)
		if err == nil {
			return cid, &message, err
		}
	}

	return cid, nil, err
}

func sendMessage(srv *server, cid int, message *bitcoin.QueryWithMessage) error {
	var packet []byte
	packet, err := json.Marshal(message)
	if err == nil {
		err = srv.lspServer.Write(cid, packet)
	}

	return err
}

func getQueryKey(queryId int, lower, upper uint64) string {
	return strconv.Itoa(queryId) + "$" + strconv.FormatUint(lower, 10) + "$" + strconv.FormatUint(upper, 10)
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "log_server.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	LOGF.Println("Server listening on port", port)

	defer srv.lspServer.Close()

	nextQueryId := 1

	for {
		cid, queryWithMessage, err := getMessage(srv)
		if err == nil {
			queryId, message := queryWithMessage.QueryId, queryWithMessage.Message

			switch message.Type {
			case bitcoin.Join:
				srv.miners = append(srv.miners, cid)
			case bitcoin.Request:
				if len(srv.miners) > 0 {
					data, lower, upper := message.Data, message.Lower, message.Upper
					stride := (upper - lower) / uint64(len(srv.miners))
					for i, miner := range srv.miners {
						new_lower := lower + stride*uint64(i)
						new_upper := lower + stride*uint64(i+1)
						if new_upper > upper {
							new_upper = upper
						}
						request := &bitcoin.QueryWithMessage{nextQueryId, *bitcoin.NewRequest(data, new_lower, new_upper)}
						sendMessage(srv, miner, request)
					}
					srv.queryCount[strconv.Itoa(nextQueryId)] = 0
					srv.queryClient[nextQueryId] = cid
					srv.queryCount[strconv.Itoa(nextQueryId)+"_jobs"] = len(srv.miners)
					nextQueryId += 1
				}
			case bitcoin.Result:
				count, exists := srv.queryCount[strconv.Itoa(queryId)]
				if exists {
					lower, upper := message.Lower, message.Upper
					_, exists = srv.queryCount[getQueryKey(queryId, lower, upper)]
					if !exists {
						count += 1
						srv.queryCount[strconv.Itoa(queryId)] = count

						serverHash := srv.queryMinHashResult[queryId]
						minerHash, minerNonce := message.Hash, message.Nonce
						if serverHash < minerHash {
							srv.queryMinHashResult[queryId] = minerHash
							srv.queryNonceResult[queryId] = minerNonce
						}

						minersCount := srv.queryCount[strconv.Itoa(queryId)+"_jobs"]
						if count == minersCount {
							LOGF.Println("[Server] Send Result Back to Client!")
							resultMessage := bitcoin.NewResult(srv.queryMinHashResult[queryId],
								srv.queryNonceResult[queryId])
							resultQueryWithMessage := &bitcoin.QueryWithMessage{queryId, *resultMessage}
							sendMessage(srv, srv.queryClient[queryId], resultQueryWithMessage)
						}
					}
				}
			}
		}
	}
}
