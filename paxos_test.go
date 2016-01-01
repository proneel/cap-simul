package cap

import (
	"flag"
	"math/rand"
	"testing"
	"time"
)

// Initialize testing. Parses flags for glog verbosity as well as seeds random numbers used in testing
func init() {
	flag.Parse()
	flag.Lookup("logtostderr").Value.Set("true")
	flag.Lookup("v").Value.Set("-1")

	// seed the random number generation
	rand.Seed(int64(time.Now().Second()))
}

// picks a count number of random agents that could be used in testing, typically by sending them control messages to modify behavior
func pickRandomNonLeaderAgents(count int) []string {
	copy := Reg.SafeCopy()
	// find the leader first
	var leaderid string
	for id, _ := range copy {
		if id[:2] == "l:" {
			leaderid = id[2:]
		}
	}

	if leaderid == "" {
		return nil
	}

	// copy all non-leader agent ids into a slice
	idlist := make([]string, 0)
	for id, _ := range copy {
		if id[2:] != leaderid && id[:2] == "a:" {
			idlist = append(idlist, id[2:])
		}
	}

	if count > len(idlist) {
		return nil // we cannot generate requested number of random agents
	}

	// keep a map that works like as hash set really
	uniqidset := make(map[string]int)

	// generate enough uniqids to satisfy the count
	for {
		// generate a random integer between 0 and len(idlist)-1 and pick that agent id
		randid := idlist[rand.Int31n(int32(len(idlist)))]
		if _, ok := uniqidset[randid]; !ok {
			uniqidset[randid] = 1 // value doesnt matter, we use it as a hashset only
			if len(uniqidset) == count {
				break
			}
		}
	}

	// copy the uniqueidset into a slice and return
	var ret = make([]string, len(uniqidset))
	i := 0
	for k := range uniqidset {
		ret[i] = k
		i++
	}

	return ret
}

// Trivial test of shortID function, used only for printing UUIDs in human readable form
func TestShortId(t *testing.T) {
	dataAndExpected := map[string]string{"abcdef": "abc", "": "<nil>", "ab": "ab"}
	for data, expected := range dataAndExpected {
		v := shortID(data, 3)
		if v != expected {
			t.Logf("Failed, expected %s got %s\n", expected, v)
			t.Fail()
		}
	}
}

// Just a plain SET and GET
func TestSimpleCommand(t *testing.T) {
	// clear out existing Registry entries before starting any test
	Reg.Clear()

	ch := make(chan Message, ChnBuffSize)
	InitAgents(3, ch)

	client := NewClient()
	client.SendCommand(SET, 10, ch)

	// sleep for 2 seconds hoping they would agree on the value
	time.Sleep(2 * time.Second)

	// get the value, it should be 10 if there is agreement
	id := client.SendCommand(GET, 0, ch) // the value in GET command is not relevant
	val, err := client.WaitForResponse(id, 5)
	if err != nil || val != 10 {
		t.Logf("Expected GET value of 10, instead err = %v, value = %v\n", err, val)
		t.Fail()
	}
}

// A SET and GET when one agent is deliberately SLOWed
func TestSlow(t *testing.T) {
	// clear out existing Registry entries before starting any test
	Reg.Clear()

	ch := make(chan Message, ChnBuffSize)
	InitAgents(3, ch)

	client := NewClient()
	// command one of the non-leaders to slow down between message processing
	randids := pickRandomNonLeaderAgents(1)
	if randids == nil {
		t.Log("Failed to select a random agent id to control!\n")
		t.FailNow()
	}

	controlch, ok := Reg.Get("t:" + randids[0])
	if !ok {
		t.Logf("Agent id %v does not have a control channel!\n", randids[0])
		t.FailNow()
	}

	client.SendControlCommand(SLOW, 2500, controlch) // this will be too SLOW to finish Paxos

	// sleep for 1 seconds while the Control gets across
	time.Sleep(1 * time.Second)

	// now do a regular SET and GET
	client.SendCommand(SET, 10, ch)

	// sleep for 2 seconds hoping they would agree on the value
	time.Sleep(2 * time.Second)

	// get the value, it should be 0 since there cannot be agreement
	id := client.SendCommand(GET, 0, ch) // the value in GET command is not relevant
	val, err := client.WaitForResponse(id, 5)
	if err != nil || val != 0 {
		t.Logf("Expected GET value of 0, instead err = %v, value = %v\n", err, val)
		t.Fail()
	}

	// but wait for a second again and do a GET, by then we should have agreement even with the SLOW agent in play
	time.Sleep(1 * time.Second)

	// get the value, it should be 10 if there is agreement
	id = client.SendCommand(GET, 0, ch) // the value in GET command is not relevant
	val, err = client.WaitForResponse(id, 5)
	if err != nil || val != 10 {
		t.Logf("Expected GET value of 10, instead err = %v, value = %v\n", err, val)
		t.Fail()
	}
}

// A SET and GET when one agent is deliberately SLOWed and another is KILLed
func TestKill(t *testing.T) {
	// clear out existing Registry entries before starting any test
	Reg.Clear()

	ch := make(chan Message, ChnBuffSize)
	InitAgents(5, ch)

	client := NewClient()
	// command one of the non-leaders to slow down between message processing
	randids := pickRandomNonLeaderAgents(2)
	if randids == nil {
		t.Log("Failed to select a random agent id to control!\n")
		t.Fail()
	}

	controlch, ok := Reg.Get("t:" + randids[0])
	if !ok {
		t.Logf("Agent id %v does not have a control channel!\n", randids[0])
		t.Fail()
	}

	client.SendControlCommand(SLOW, 2500, controlch) // this will be too SLOW to finish Paxos

	// command another of the non-leaders to terminate
	controlch, ok = Reg.Get("t:" + randids[1])
	if !ok {
		t.Logf("Agent id %v does not have a control channel!\n", randids[1])
		t.Fail()
	}

	client.SendControlCommand(KILL, 0, controlch) // kill this agent

	// sleep for 1 seconds while the Control gets across
	time.Sleep(1 * time.Second)

	// now do a regular SET and GET
	client.SendCommand(SET, 10, ch)

	// sleep for 2 seconds hoping they would agree on the value
	time.Sleep(2 * time.Second)

	// get the value, it should be 0 since there cannot be agreement
	id := client.SendCommand(GET, 0, ch) // the value in GET command is not relevant
	val, err := client.WaitForResponse(id, 5)
	if err != nil || val != 0 {
		t.Logf("Expected GET value of 0, instead err = %v, value = %v\n", err, val)
		t.Fail()
	}

	// but wait for a second again and do a GET, by then we should have agreement even with the SLOW agent in play
	time.Sleep(1 * time.Second)

	// get the value, it should be 10 if there is agreement
	id = client.SendCommand(GET, 0, ch) // the value in GET command is not relevant
	val, err = client.WaitForResponse(id, 5)
	if err != nil || val != 10 {
		t.Logf("Expected GET value of 10, instead err = %v, value = %v\n", err, val)
		t.Fail()
	}
}

// A SET and GET when one agent is SUSPENDed
func TestSuspend(t *testing.T) {
	// clear out existing Registry entries before starting any test
	Reg.Clear()

	ch := make(chan Message, ChnBuffSize)
	InitAgents(3, ch)

	client := NewClient()
	// command one of the non-leaders to suspend message processing for 3 seconds
	randids := pickRandomNonLeaderAgents(1)
	if randids == nil {
		t.Log("Failed to select a random agent id to control!\n")
		t.Fail()
	}

	controlch, ok := Reg.Get("t:" + randids[0])
	if !ok {
		t.Logf("Agent id %v does not have a control channel!\n", randids[0])
		t.Fail()
	}

	client.SendControlCommand(SUSPEND, 3500, controlch) // this will not finish Paxos

	// sleep for 1 seconds while the Control gets across
	time.Sleep(1 * time.Second)

	// now do a regular SET and GET
	client.SendCommand(SET, 10, ch)

	// sleep for 2 seconds hoping they would agree on the value
	time.Sleep(2 * time.Second)

	// get the value, it should be 0 since there cannot be agreement
	id := client.SendCommand(GET, 0, ch) // the value in GET command is not relevant
	val, err := client.WaitForResponse(id, 5)
	if err != nil || val != 0 {
		t.Logf("Expected GET value of 0, instead err = %v, value = %v\n", err, val)
		t.Fail()
	}

	// but wait for a couple second again and do a GET, by then we should have agreement since the suspended agent would be back in play
	time.Sleep(2 * time.Second)

	// get the value, it should be 10 if there is agreement
	id = client.SendCommand(GET, 0, ch) // the value in GET command is not relevant
	val, err = client.WaitForResponse(id, 5)
	if err != nil || val != 10 {
		t.Logf("Expected GET value of 10, instead err = %v, value = %v\n", err, val)
		t.Fail()
	}
}
