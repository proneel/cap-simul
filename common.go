package cap

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/satori/go.uuid"
)

// A generic message interface. Defined so we can send various kinds of messages on channels
type Message interface {
	Id() string
}

// A Registry is a thread-safe map. It keeps a map of entities (clients, agents) and the channels used to communicate with them.
type Registry struct {
	entities map[string]chan Message
	sync.RWMutex
}

// A global instance of the Registry
var Reg Registry

// initialize the global Registry
func init() {
	Reg.entities = make(map[string]chan Message)
}

// Thread safe insert/update of map
func (r *Registry) Set(key string, val chan Message) {
	r.Lock()
	defer r.Unlock()
	r.entities[key] = val
}

// Thread safe retrieval of map
func (r *Registry) Get(key string) (val chan Message, ok bool) {
	r.RLock()
	defer r.RUnlock()
	val, ok = r.entities[key]
	return
}

// A thread-safe clone of the Registry which can then be iterated over
func (r *Registry) SafeCopy() (copy map[string]chan Message) {
	r.RLock()
	defer r.RUnlock()
	copy = make(map[string]chan Message)
	for k, v := range r.entities {
		copy[k] = v
	}
	return
}

// Removes all entries from the Registry
func (r *Registry) Clear() {
	r.Lock()
	defer r.Unlock()
	for k, _ := range r.entities {
		delete(r.entities, k)
	}
}

type CommandType int

// Client command types
const (
	GET CommandType = iota
	SET
	ADD
	MULTIPLY
	RESPONSE // a response from the agent to the client
)

// A ClientMessage is used by a Client to send state change requests to a Leader agent. Also, used by that Agent to send responses back to the Client.
type ClientMessage struct {
	id        string      // needs to be unique and thread-safe without locking, probably uuid
	requester string      // id of the requester
	cType     CommandType // GET, SET etc
	val       int         // a value to SET or value to be ADDed or MULTIPLYed
}

// ClientMessage implements the Message interface
func (c ClientMessage) Id() string {
	return c.id
}

// Special String function for readability
func (c ClientMessage) String() string {
	var cType = "Unknown message type"
	switch c.cType {
	case GET:
		cType = "GET"
	case SET:
		cType = "SET"
	case ADD:
		cType = "ADD"
	case MULTIPLY:
		cType = "MULTIPLY"
	case RESPONSE:
		cType = "RESPONSE"
	}

	return fmt.Sprintf("{%s; id:%s requester:%s value:%v}", cType, shortID(c.id, 3), shortID(c.requester, 3), c.val)
}

type ControlCmdType int

// A control command could be used to simulate Agent failures to test Paxos stability in the face of failures
const (
	NORMAL  ControlCmdType = iota // agent to behave normally
	SUSPEND                       // agent to suspend for a while
	KILL                          // kill/terminate an agent
	SLOW                          // slow down receiving and responding to messages
)

// A control command has a control type (KILL, SUSPEND etc) and a value (when applicable)
type ControlMessage struct {
	id    string // needs to be unique and thread-safe without locking, probably uuid
	cType ControlCmdType
	val   uint // control value, applies only to SLOW (how slow in msecs) and SUSPEND (for how long to suspend)
}

// ControlMessage implements the Message interface
func (c ControlMessage) Id() string {
	return c.id
}

// Special String function for readability
func (c ControlMessage) String() string {
	var cType = "Unknown message type"
	switch c.cType {
	case NORMAL:
		cType = "NORMAL"
	case SUSPEND:
		cType = "SUSPEND"
	case KILL:
		cType = "KILL"
	case SLOW:
		cType = "SLOW"
	}

	return fmt.Sprintf("{%s; id:%s value:%v}", cType, shortID(c.id, 3), c.val)
}

// A Client is just a unique client id and a channel on which it can receive RESPONSEs back from the Agents (typically Leader)
type Client struct {
	Id   string
	inch chan Message
}

// Creates a new Client and registers it in the Registry. Returns a new Client.
func NewClient() *Client {
	c := Client{Id: uuid.NewV4().String(), inch: make(chan Message, ChnBuffSize)}
	// register this client, client ids are prefixed with c: in registry
	Reg.Set("c:"+c.Id, c.inch)
	return &c
}

// Sends a ClientMessage on the outbound channel for the Client, with the expectation that an Agent Leader would be listening on it
// The channels have buffers, so unless the buffer is full, the function returns immediately. But if the buffer is full, it could timeout in 5 secs.
func (c *Client) SendCommand(cType CommandType, val int, ch chan Message) string {
	id := uuid.NewV4().String()
	m := ClientMessage{id: id, requester: c.Id, cType: cType, val: val}
	// send the message asynchronously
	glog.V(2).Infof("Going to send from Client on %v\n", ch)
	asyncSend(ch, m, 5000)

	glog.V(2).Infof("Send from Client done id = %v\n", id)
	return id
}

// Waits for a response from the Agent on the Client's inbound channel.
// Drops any messages which does not have its id as the request id provided. Times out in timeoutSecs.
func (c *Client) WaitForResponse(id string, timeoutSecs int) (int, error) {
	timerC := time.NewTimer(time.Duration(timeoutSecs) * time.Second).C

	for {
		select {
		case <-timerC:
			return 0, errors.New("Timed Out.")
		case r := <-c.inch:
			if r.Id() != id {
				continue // not the response for the message we are looking for? Should we push it back?
			} else {
				return r.(ClientMessage).val, nil
			}
		}
	}
}

// Sends a ControlMessage on the inbound channel for an Agent, with the expectation that the Agent will perform the Control Action. The Agent does not respond back.
// The channels have buffers, so unless the buffer is full, the function returns immediately. But if the buffer is full, it could timeout in 5 secs.
func (c *Client) SendControlCommand(cType ControlCmdType, val uint, ch chan Message) string {
	id := uuid.NewV4().String()
	m := ControlMessage{id: id, cType: cType, val: val}
	// send the message asynchronously
	glog.V(2).Infof("Going to send from Client on %v\n", ch)
	asyncSend(ch, m, 5000)
	glog.V(3).Infof("Send from Client done id = %v\n", id)
	return id
}

// Utility method to get just a substring of an id, UUIDs are very long and a short subset is easier to read when printed.
func shortID(id string, length uint) string {
	if id == "" {
		return "<nil>"
	} else {
		// take a subset of the id, accounting for if the requested length is longer than the length of the id
		return id[:uint(math.Min(float64(length), float64(len(id))))]
	}
}

// send a Message on a channel and timeout in timeoutMSec if the send has not succeeded
// returns nil if successful and an error in case of failure
func asyncSend(ch chan Message, m Message, timeoutMsec uint) error {
	tC := time.NewTimer(time.Duration(timeoutMsec) * time.Millisecond).C
	select {
	case ch <- m:
	case <-tC:
		glog.Errorf("Timed out on sending message %v\n", m)
		return errors.New("Timed out")
	}

	return nil
}
