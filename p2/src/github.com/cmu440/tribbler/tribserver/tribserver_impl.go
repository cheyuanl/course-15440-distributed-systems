package tribserver

import (
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
)

const MAX_TRIBBLES_COUNT = 100

type tribServer struct {
	libstore libstore.Libstore
}

type Tribbles []tribrpc.Tribble

func (slice Tribbles) Len() int {
	return len(slice)
}

func (slice Tribbles) Less(i, j int) bool {
	return slice[i].Posted.UnixNano() > slice[j].Posted.UnixNano()
}

func (slice Tribbles) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	ts := new(tribServer)
	libstore, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
	if err != nil {
		return ts, err
	}
	ts.libstore = libstore

	rpc.RegisterName("TribServer", tribrpc.Wrap(ts))
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}
	go http.Serve(l, nil)

	return ts, nil
}

func (ts *tribServer) ExistsUser(userID string) bool {
	key := util.FormatUserKey(userID)
	_, err := ts.libstore.Get(key)
	if err == nil {
		return true
	} else {
		return false
	}
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	exists := ts.ExistsUser(args.UserID)
	if exists {
		reply.Status = tribrpc.Exists
		return nil
	}

	key := util.FormatUserKey(args.UserID)
	err := ts.libstore.Put(key, "")
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK

	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	if !ts.ExistsUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	if !ts.ExistsUser(args.TargetUserID) {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	var key = util.FormatSubListKey(args.UserID)
	err := ts.libstore.AppendToList(key, args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.Exists
	} else {
		reply.Status = tribrpc.OK
	}

	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	if !ts.ExistsUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	if !ts.ExistsUser(args.TargetUserID) {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	var key = util.FormatSubListKey(args.UserID)
	err := ts.libstore.RemoveFromList(key, args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	reply.Status = tribrpc.OK

	return nil
}

func (ts *tribServer) IsSubscribed(userID string, targetUserID string) bool {
	var key = util.FormatSubListKey(userID)
	err := ts.libstore.AppendToList(key, targetUserID)
	if err != nil {
		return true
	} else {
		return false
	}
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	if !ts.ExistsUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	var key = util.FormatSubListKey(args.UserID)
	users, err := ts.libstore.GetList(key)
	if err != nil {
		reply.Status = tribrpc.OK
		return nil
	}
	friends := []string{}
	for _, user := range users {
		if ts.IsSubscribed(user, args.UserID) {
			friends = append(friends, user)
		}
	}
	reply.Status = tribrpc.OK
	reply.UserIDs = friends

	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	if !ts.ExistsUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	var postTime = time.Now().UnixNano()
	var postKey = util.FormatPostKey(args.UserID, postTime)
	err := ts.libstore.Put(postKey, args.Contents)
	if err != nil {
		return err
	}

	var tribListKey = util.FormatTribListKey(args.UserID)
	err = ts.libstore.AppendToList(tribListKey, postKey)
	if err != nil {
		return err
	}

	reply.Status = tribrpc.OK
	reply.PostKey = postKey

	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	if !ts.ExistsUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	var tribListKey = util.FormatTribListKey(args.UserID)
	err := ts.libstore.RemoveFromList(tribListKey, args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}

	err = ts.libstore.Delete(args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return err
	}

	reply.Status = tribrpc.OK

	return nil
}

func (ts *tribServer) GetTribblesOfUser(userID string) []tribrpc.Tribble {
	var tribListKey = util.FormatTribListKey(userID)
	tribKeys, err := ts.libstore.GetList(tribListKey)
	if err != nil {
		return []tribrpc.Tribble{}
	}

	// reverse
	var tribs = Tribbles{}
	for _, tribKey := range tribKeys {
		postTime := time.Unix(0, util.GetPostTime(userID, tribKey))
		var contents string
		contents, _ = ts.libstore.Get(tribKey)
		tribs = append(tribs, tribrpc.Tribble{userID, postTime, contents})
	}

	sort.Sort(tribs)
	if len(tribs) > MAX_TRIBBLES_COUNT {
		tribs = tribs[:MAX_TRIBBLES_COUNT]
	}

	return tribs
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	if !ts.ExistsUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	var tribs = ts.GetTribblesOfUser(args.UserID)

	reply.Status = tribrpc.OK
	reply.Tribbles = tribs

	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	if !ts.ExistsUser(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	var key = util.FormatSubListKey(args.UserID)
	users, err := ts.libstore.GetList(key)
	if err != nil {
		reply.Status = tribrpc.OK
		return nil
	}

	tribs := Tribbles{}
	for _, user := range users {
		tribs = append(tribs, ts.GetTribblesOfUser(user)...)
	}

	sort.Sort(tribs)
	if len(tribs) > MAX_TRIBBLES_COUNT {
		tribs = tribs[:MAX_TRIBBLES_COUNT]
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = tribs

	return nil
}
