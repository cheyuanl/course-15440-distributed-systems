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
	lspServer lsp.Server
	miners    []int
}

func startServer(port int) (*server, error) {
	lspServer, err := lsp.NewServer(port, bitcoin.MakeParams(5, 2000, 1))
	if err != nil {
		return nil, err
	}

	srv := &server{
		lspServer,
		make([]int, 0)}

	return srv, nil
}

func getMessage(srv *server) (int, *bitcoin.Message, error) {
	cid, payload, err := srv.lspServer.Read()
	if err == nil {
		var message bitcoin.Message
		err = json.Unmarshal(payload, &message)
		if err == nil {
			return cid, &message, err
		}
	}

	return cid, nil, err
}

func sendMessage(srv *server, cid int, message *bitcoin.Message) error {
	var packet []byte
	packet, err := json.Marshal(message)
	if err == nil {
		err = srv.lspServer.Write(cid, packet)
	}

	return err
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "log.txt"
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
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()

	for {
		cid, message, err := getMessage(srv)
		if err == nil {
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
						request := bitcoin.NewRequest(data, new_lower, new_upper)
						sendMessage(srv, miner, request)
					}
				}
			case bitcoin.Result:
			}
		}
	}
}
