package cap

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/satori/go.uuid"
)

// Buffer size of channels used in inter-agent processing. Keep it high enough so messages are mostly asynch, but not too high to be a memory hog
const ChnBuffSize int = 100

// Type of Paxos message being sent between participants
type PxMessageType int

// Standard values for MessageTypes
const (
	Prepare PxMessageType = iota
	Promise
	AcceptRequest
	Accepted
)

// The Paxos protocol messages sent between agents
type PxMessage struct {
	id             string // a UUID uniquely identifying the message
	mType          PxMessageType
	fromID         string // the ID of the sender of the message, be it proposer or acceptor
	toID           string // the ID of the receiver of the message, be it proposer or acceptor
	proposalID     uint   // the proposal ID in question in this message
	lastAcceptedID uint   // for a Promise message, the last ID this acceptor had accepted
	value          int    // for a Promise message, the last value this acceptor had accepted
}

// PxMessage implements the Message interface
func (m PxMessage) Id() string {
	return m.id
}

// Special String function for readability
func (m PxMessage) String() string {
	switch m.mType {
	case Prepare:
		return fmt.Sprintf("{Prepare; proposer:%s acceptor:%s proposalID:%v}", shortID(m.fromID, 3), shortID(m.toID, 3), m.proposalID)
	case Promise:
		return fmt.Sprintf("{Promise; acceptor:%s proposer:%s proposalID:%v acceptedID:%v value:%v}", shortID(m.fromID, 3), shortID(m.toID, 3), m.proposalID, m.lastAcceptedID, m.value)
	case AcceptRequest:
		return fmt.Sprintf("{AcceptRequest; proposer:%s acceptor:%s proposalID:%v value:%v}", shortID(m.fromID, 3), shortID(m.toID, 3), m.proposalID, m.value)
	case Accepted:
		return fmt.Sprintf("{Accepted; acceptor:%s learner:%s proposalID:%v value:%v}", shortID(m.fromID, 3), shortID(m.toID, 3), m.proposalID, m.value)
	default:
		return "Unknown message type"
	}
}

// structure used internally by agents to figure out when a value is agreed upon (learned)
type LearningValue struct {
	numAccepted uint // how many acceptors have accepted a given proposal
	numObsolete uint // how many of the above acceptors have accepted a newer proposal making this obsolete
	value       int  // value learned
}

// Agent data including the simple state machine
type Agent struct {
	Id             string
	leaderId       string
	inch           chan Message // paxos message passing between agents
	clientinch     chan Message // messages from client to leader
	controlch      chan Message // control messages to this agent to make it slow/suspend etc
	numAgents      uint
	agentNumber    uint
	nextProposalId uint // next one-up number to be assigned to a proposal from this proposer, incremement by numAgents each time!
	slowMsec       uint // if we were sent a control message to be SLOW, this would denote the msecs to slow down for

	// fields used by the Proposer element of the Agent, prefixed with 'p' to indicate it is used in the proposer context
	pActiveID    uint            // the proposal id currently active from this proposer
	pActiveVal   int             // the value being currently proposed (active)
	pMaxAcceptID uint            // an acceptor may send an ID in a promise which is > the current proposers id, in which case the proposer should honor that value
	pPromises    map[string]uint // a map of acceptorID:proposalID who have promised this acceptor on the active proposal

	// fields used by the Acceptor element of the Agent, prefixed with 'a' to indicate it is used in the acceptor context
	aPromisedID  uint // the proposal id that this acceptor last promised to any proposer
	aAcceptedID  uint // the proposal id that this acceptor last accepted
	aAcceptedVal int  // the value that this acceptor last accepted

	// fields used by the Learner element of the Agent, prefixed with 'l' to indicate it is used in the learner context
	lProposals       map[uint]*LearningValue // a map of what has so far been learnt on a given proposal
	lAcceptors       map[string]uint         // a map of what acceptors have accepted which proposal (latest only)
	lFinalValue      int                     // finally learnt value
	lFinalProposalId uint                    // final proposal which can be deemed as learnt
}

// Initialize numAgents number of agents. Spins each agent off into its own goroutine for concurrent processing
// Only the leader will listen for client commands on clientOutch
// All Agents are registered in the registry for their listening channel for inter-agent communication as well as for control messages
func InitAgents(numAgents uint, clientOutch chan Message) {
	// first generate the leader id
	leaderId := uuid.NewV4().String()

	// calculate the starting base proposal number for each agent, although it is unlikely any other agent will use it unless they become the leader
	// we pick a number that is a multiple of the number of agents (for dividing) and also higher than any other proposal number we have used (use 100 in Init)
	baseProposal := uint(100)
	baseProposal = ((baseProposal / numAgents) + 1) * numAgents

	// generate each agent, the first will use the leaderId as its id, other agents will have generated ids
	// also each agent will have a different nextProposalId, which means that if they keep adding numAgents to it each time a proposal is sent by them, the numbers will never clash
	// and yet any agent could in theory have a higher proposal number (at that time) than the others
	for i := uint(0); i < numAgents; i++ {
		var a Agent
		if i == uint(0) {
			a = Agent{Id: leaderId, leaderId: leaderId, inch: make(chan Message, ChnBuffSize), clientinch: clientOutch, numAgents: numAgents, agentNumber: 0, nextProposalId: baseProposal, controlch: make(chan Message, 5), slowMsec: 0}
			// register the leader separately prefixed with l: in registry
			Reg.Set("l:"+a.Id, a.inch)
		} else {
			a = Agent{Id: uuid.NewV4().String(), leaderId: leaderId, inch: make(chan Message, ChnBuffSize), clientinch: clientOutch, numAgents: numAgents, agentNumber: i, nextProposalId: baseProposal, controlch: make(chan Message, 5), slowMsec: 0}
		}

		// register this agent, agent ids are prefixed with a: in registry
		Reg.Set("a:"+a.Id, a.inch)
		// the control channel for this same agent is prefixed with t: in registry
		Reg.Set("t:"+a.Id, a.controlch)

		// kick off a goroutine to process the lifecycle of this agent
		go a.lifecycle()
	}
}

// lifecycle of an agent. runs forever till KILLed by a control message.
// Only the leader listens for client messages, but all agents listen on inter-agent communication as well as control messages
func (a *Agent) lifecycle() {
	// process messages forever until perhaps a KILL message
	for {
		// if I am the leader (or believe I am the leader) I will listen on the clientinch as well as on messages from other agents and control messages
		if a.Id == a.leaderId {
			select {
			case tm := <-a.controlch:
				if a.processControlMessage(tm.(ControlMessage)) {
					return // we were sent a KILL
				}
			case cm := <-a.clientinch:
				a.processClientMessage(cm.(ClientMessage))
			case am := <-a.inch:
				a.processAgentMessage(am.(PxMessage))
			}
		} else { // listen only for inter-agent communication and control messages
			glog.V(2).Infof("Agent %s to listen on channel %v\n", a.Id, a.inch)
			select {
			case tm := <-a.controlch:
				if a.processControlMessage(tm.(ControlMessage)) {
					return // we were sent a KILL
				}
			case am := <-a.inch:
				a.processAgentMessage(am.(PxMessage))
			}
		}
	}
}

// Processes a client command to GET or SET a value. ADD and MULTIPLY operate on the current value of the state machine
func (a *Agent) processClientMessage(m ClientMessage) {
	glog.V(1).Info(fmt.Sprintf("Agent %s: Received client message %+v\n", shortID(a.Id, 3), m))
	switch m.cType {
	case GET:
		// send the latest learned value to the client
		a.sendToClient(m.id, m.requester, a.lFinalValue)
	case SET:
		a.propose(m.val)
	case ADD:
		a.propose(m.val + a.lFinalValue)
	case MULTIPLY:
		a.propose(m.val * a.lFinalValue)
	}
}

// Processes an inter-agent communication message. These are all Paxos protocol messages
func (a *Agent) processAgentMessage(m PxMessage) {
	glog.V(1).Info(fmt.Sprintf("Agent %s: Received agent message %+v\n", shortID(a.Id, 3), m))
	switch m.mType {
	case Prepare:
		a.recvPrepare(m.fromID, m.proposalID)
	case Promise:
		a.recvPromise(m.fromID, m.proposalID, m.lastAcceptedID, m.value)
	case AcceptRequest:
		a.recvAcceptRequest(m.proposalID, m.value)
	case Accepted:
		a.recvAccepted(m.fromID, m.proposalID, m.value)
	}

	if a.slowMsec > 0 {
		// we were sent a control message to slow down. Lets wait for the required time before we go back to listening on the channel
		time.Sleep(time.Duration(a.slowMsec) * time.Millisecond)
	}
}

// Processes a control message from an external source which mimics failure conditions in the Paxos routine.
// The external source can make an agent SUSPEND processing for a while, KILL it completely, SLOW down processing and even return it back to NORMAL operation.
func (a *Agent) processControlMessage(m ControlMessage) (exit bool) {
	glog.V(0).Info(fmt.Sprintf("Agent %s: Received control message %+v\n", shortID(a.Id, 3), m))
	switch m.cType {
	case NORMAL:
		a.slowMsec = 0 // 0 indicates we wont slow down any more
	case SUSPEND:
		time.Sleep(time.Duration(m.val) * time.Millisecond)
	case KILL:
		return true // terminate this agent, true will tell the agent for loop to quit
	case SLOW:
		a.slowMsec = m.val // slow down each message processing by requested amount
	}
	return false // we dont have to exit/terminate this agent
}

// Sends a RESPONSE from the agent (typically leader) to the client
func (a *Agent) sendToClient(respid string, clientid string, val int) {
	// find the client from the Registry
	ch, ok := Reg.Get("c:" + clientid)
	if !ok {
		glog.Errorf("Agent %s tried to send a message to %s for req/resp id %s and the Client is no longer in the registry!\n", a.Id, clientid, respid)
		return
	}
	// send the message to the client asynchronously
	m := ClientMessage{id: respid, requester: clientid, cType: RESPONSE, val: val}
	asyncSend(ch, m, 5000)
}

// We got a proposal request for a new value from the Client.
// Run Paxos on this new value hoping for (but not guaranteeing) consensus.
func (a *Agent) propose(value int) {
	if a.pActiveVal == 0 {
		// we do not have an active proposal under way, get a new proposal ID and store the requested value
		a.pActiveVal, a.pActiveID, a.pMaxAcceptID, a.pPromises = value, a.nextProposalId, a.nextProposalId, make(map[string]uint)
		// to keep each proposal number monotonically increasing as well as unique across agents, we increment our proposal number not by 1 each time but by numAgents
		a.nextProposalId += a.numAgents

		// Quoting from Wikipedia:
		// A Proposer (the leader) creates a proposal identified with a number N. This number must be greater than any previous proposal number used by this Proposer.
		// Then, it sends a Prepare message containing this proposal to a Quorum of Acceptors. The Proposer decides who is in the Quorum.

		// now send a Prepare message to *all* Acceptors (other Agents), not just a quorum
		a.sendPrepare(a.Id, a.pActiveID)
	}

	// TODO: Send error back to client or just drop -> There is already an active proposal. Cannot Initiate Another.
}

// Sends a Prepare message from a proposer to all other agents (who act as acceptors)
func (a *Agent) sendPrepare(proposerID string, proposalID uint) {
	// send it asynchronously on the channel(s) to *each* acceptor except this one
	for id, ch := range Reg.SafeCopy() {
		if id[:2] == "a:" && id != "a:"+proposerID {
			// create a Message to send to this acceptor
			msg := PxMessage{mType: Prepare, fromID: proposerID, toID: id[2:], proposalID: proposalID}

			glog.V(3).Infof("Proposer %s with channel %v Going to send a Prepare to each acceptor %s on channel %v\n", a.Id, a.inch, id, ch)
			asyncSend(ch, msg, 5000)
		}
	}
}

// An agent (behaving as an acceptor in this message) receives a Prepare message from a proposer and processes it
func (a *Agent) recvPrepare(proposerID string, proposalID uint) {
	// Quoting from Wikipedia:
	// If the proposal's number N is higher than any previous proposal number received from any Proposer by the Acceptor,
	// then the Acceptor must return a promise to ignore all future proposals having a number less than N.
	// If the Acceptor accepted a proposal at some point in the past, it must include the previous proposal number and previous value in its response to the Proposer.
	// Otherwise, the Acceptor can ignore the received proposal. It does not have to answer in this case for Paxos to work. However, for the sake of optimization,
	// sending a denial (Nack) response would tell the Proposer that it can stop its attempt to create consensus with proposal N.
	if proposalID >= a.aPromisedID {
		a.aPromisedID = proposalID
		// create a Message to send to the proposer
		msg := PxMessage{mType: Promise, fromID: a.Id, toID: proposerID, proposalID: proposalID, lastAcceptedID: a.aAcceptedID, value: a.aAcceptedVal}
		if ch, ok := Reg.Get("a:" + proposerID); !ok {
			glog.Errorf("Cannot find registration for agent %s just when agent %s is ready to send promise\n", proposerID, a.Id)
		} else {
			glog.V(3).Infof("Acceptor %s with channel %v going to send a promise to proposer %s on channel %v\n", a.Id, a.inch, proposerID, ch)
			asyncSend(ch, msg, 5000)
		}
	}
}

// An agent (behaving as a proposer in this message) receives a Promise message from an acceptor and processes it
func (a *Agent) recvPromise(acceptorID string, proposalID uint, acceptedId uint, acceptedValue int) {
	// we receive a promise from one of the acceptors

	// Quoting from wikipedia:
	// If a Proposer receives enough promises from a Quorum of Acceptors, it needs to set a value to its proposal.
	// If any Acceptors had previously accepted any proposal, then they'll have sent their values to the Proposer, who now must set the value of its proposal to the value associated with the highest proposal number reported by the Acceptors.
	// If none of the Acceptors had accepted a proposal up to this point, then the Proposer may choose any value for its proposal.
	// The Proposer sends an Accept Request message to a Quorum of Acceptors with the chosen value for its proposal.

	// ignore the promise if it doesnt correspond to the proposal we sent a prepare for, also ignore if we already received a message from this acceptor
	// also ignore it if we have already received enough promises, we would have sent out AcceptRequests to the quorum anyway

	quorum := int((a.numAgents / 2) + 1) // a simple majority
	if _, found := a.pPromises[acceptorID]; proposalID != a.pActiveID || found || len(a.pPromises) >= quorum {
		return
	}

	a.pPromises[acceptorID] = proposalID // save the fact that we got a promise from this acceptor

	// if the acceptor has accepted a previous higher id, we need to need to use the value from that, not the one we proposed
	if acceptedId > a.pMaxAcceptID {
		a.pMaxAcceptID = acceptedId
		if acceptedValue != 0 { // TODO: fix this bad bad assumption. acceptedValue of 0 is really valid as a set value, need to find another way of indicating unset previously
			a.pActiveVal = acceptedValue
		}
	}

	// if we have received a quorum of values, we can send an accept request to the acceptors who made the promise
	if (len(a.pPromises)) >= quorum {
		// make a slice of acceptor ids (the quorum list)
		acceptorIDs := make([]string, len(a.pPromises))
		i := 0
		for k := range a.pPromises {
			acceptorIDs[i] = k
			i++
		}

		a.sendAcceptRequest(a.Id, acceptorIDs, a.pActiveID, a.pActiveVal)
	}
}

// A Proposer sends an AcceptRequest to each acceptor who has (so far) given a Promise for a previous Proposal
func (a *Agent) sendAcceptRequest(proposerID string, acceptorIDs []string, proposalID uint, proposedValue int) {
	// send it asynchronously on the channel(s) to *each* acceptor except this one
	for id, ch := range Reg.SafeCopy() {
		if id[:2] == "a:" && id != "a:"+proposerID {
			// create a Message to send to this acceptor
			msg := PxMessage{mType: AcceptRequest, fromID: proposerID, toID: id[2:], proposalID: proposalID, value: proposedValue}
			glog.V(3).Infof("Proposer %s with channel %v Going to send a AcceptRequest to each acceptor %s on channel %v\n", a.Id, a.inch, id, ch)
			asyncSend(ch, msg, 5000)
		}
	}
}

// An Acceptor receives an AcceptRequest for a Promise it has made earlier
func (a *Agent) recvAcceptRequest(proposalID uint, acceptedValue int) {
	// Quoting from Wikipedia:
	// If an Acceptor receives an Accept Request message for a proposal N, it must accept it if and only if it has not already promised to any prepare proposals having an identifier greater than N.
	// In this case, it should register the corresponding value v and send an Accepted message to the Proposer and every Learner. Else, it can ignore the Accept Request.
	// Note that an Acceptor can accept multiple proposals. These proposals may even have different values in the presence of certain failures.
	// However, the Paxos protocol will guarantee that the Acceptors will ultimately agree on a single value.

	// We might even receive an acceptrequest for a promise we never gave, but that is ok if the proposal number is greater than the last promised id we have
	if proposalID >= a.aPromisedID {
		a.aPromisedID, a.aAcceptedID, a.aAcceptedVal = proposalID, proposalID, acceptedValue

		// send the accepted message to all agents
		a.sendAccepted(proposalID, acceptedValue)
	}
}

// An Acceptor sends an Accepted message to all agents so any Learners and Proposers amongst them know that this agent has accepted the value
func (a *Agent) sendAccepted(proposalID uint, proposedValue int) {
	// send it asynchronously on the channel(s) to *all* agents (proposers, learners) *including* this one (who also behaves as a learner)
	for id, ch := range Reg.SafeCopy() {
		if id[:2] == "a:" {
			// create a Message to send to this agent
			msg := PxMessage{mType: Accepted, fromID: a.Id, toID: id[2:], proposalID: proposalID, value: proposedValue}
			glog.V(3).Infof("Acceptor %s with channel %v Going to send a Accepted to each agent %s on channel %v\n", a.Id, a.inch, id, ch)
			asyncSend(ch, msg, 5000)
		}
	}
}

// An agent wearing a Learner hat receives the Accepted message from an agent and processes it
func (a *Agent) recvAccepted(acceptorID string, proposalID uint, value int) {
	// A learner has received an Accepted message from an acceptor
	// if we have already a final learned value, just ignore it (TODO: how do we restart when a new SET begins?)
	if a.lFinalValue != 0 { // TODO: fix this bad bad assumption. lFinalValue of 0 is really valid as a set value, need to find another way of indicating finalValue hasnt been realized yet
		return
	}

	if a.lAcceptors == nil {
		// first accept, initialize maps
		a.lAcceptors = make(map[string]uint)
		a.lProposals = make(map[uint]*LearningValue)
	}

	// find the last id accepted by this acceptor, if this proposal is older than or equal to the last one accepted by this acceptor (message could be late or duplicate) ignore it
	lastAcceptedID, ok := a.lAcceptors[acceptorID]
	if ok && lastAcceptedID <= proposalID {
		return
	}

	// mark this proposal as the latest from this acceptor
	a.lAcceptors[acceptorID] = proposalID

	// if we had a previous proposal accepted by this acceptor, we need to clean it up from our proposal list and even delete the proposal if all acceptors think it is old
	if ok {
		if lv, ok2 := a.lProposals[lastAcceptedID]; ok2 {
			lv.numObsolete -= 1      // we accepted it, now we reduce 1 to mark as obsolete
			if lv.numObsolete == 0 { // this was the last obsoleted acceptor, this proposal is dead
				delete(a.lProposals, lastAcceptedID)
			}
		}
	}

	// if this is the first we've heard of this proposal from an acceptor, create an entry in the map
	if _, ok := a.lProposals[proposalID]; !ok {
		a.lProposals[proposalID] = &LearningValue{0, 0, value}
	}

	// get the Learning value reference and increment the counters
	lv := a.lProposals[proposalID]
	lv.numAccepted++
	lv.numObsolete++
	if lv.value != value {
		glog.Error("Different values %v and %v learned for same proposal id %v\n", value, lv.value, proposalID)
	}

	// if this is the accepted that exactly puts us at quorom, then we have learned the final value!
	quorum := uint((a.numAgents / 2) + 1) // a simple majority
	if lv.numAccepted == quorum {
		a.lFinalProposalId = proposalID
		a.lFinalValue = value
		a.lAcceptors = nil
		a.lProposals = nil
		glog.V(0).Infof("Learner %s learned final value %v on proposal id %v\n", a.Id, value, proposalID)
	}
}
