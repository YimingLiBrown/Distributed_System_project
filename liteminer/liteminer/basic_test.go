package liteminer

import (
	"testing"
)

func TestBasic(t *testing.T) {
	p, err := CreatePool("")
	if err != nil {
		t.Errorf("Received error %v when creating pool", err)
	}

	addr := p.Addr.String()

	numMiners := 2
	miners := make([]*Miner, numMiners)
	for i := 0; i < numMiners; i++ {
		m, err := CreateMiner(addr)
		if err != nil {
			t.Errorf("Received error %v when creating miner", err)
		}
		miners[i] = m
	}

	client := CreateClient([]string{addr})
	p.GetClient()
	p.GetMiners()
	data := "data"
	upperbound := uint64(5000000)
	nonces, err := client.Mine(data, upperbound)
	if err != nil {
		t.Errorf("Received error %v when mining", err)
	} else {
		for _, nonce := range nonces {
			expected := int64(2042628)
			if nonce != expected {
				t.Errorf("Expected nonce %d, but received %d", expected, nonce)
			}
		}
	}
}

func TestMiners(t *testing.T) { //shutdown all the miners
	p, err := CreatePool("")
	if err != nil {
		t.Errorf("Received error %v when creating pool", err)
	}

	addr := p.Addr.String()

	numMiners := 6
	miners := make([]*Miner, numMiners)
	for i := 0; i < numMiners; i++ {
		m, err := CreateMiner(addr)
		if err != nil {
			t.Errorf("Received error %v when creating miner", err)
		}
		miners[i] = m
		miners[i].Shutdown()
	}
	client := CreateClient([]string{addr})

	data := "data"
	upperbound := uint64(5000000)
	nonces, err := client.Mine(data, upperbound)
	if err != nil {
		t.Errorf("Received error %v when mining", err)
	} else {
		for _, nonce := range nonces {
			expected := int64(2042628)
			if nonce != expected {
				t.Errorf("Expected nonce %d, but received %d", expected, nonce)
			}
		}
	}
}

func TestClients(t *testing.T) { //connect 2 pools to the same client
	p1, err1 := CreatePool("")
	p2, err2 := CreatePool("")

	if err1 != nil {
		t.Errorf("Received error %v when creating pool", err1)
	}

	if err2 != nil {
		t.Errorf("Received error %v when creating pool", err2)
	}

	addr1 := p1.Addr.String()
	addr2 := p2.Addr.String()
	numMiners := 2
	miners1 := make([]*Miner, numMiners)
	for i := 0; i < numMiners; i++ {
		m, err1 := CreateMiner(addr1)
		if err1 != nil {
			t.Errorf("Received error %v when creating miner", err1)
		}
		miners1[i] = m
	}

	miners2 := make([]*Miner, numMiners)
	for i := 0; i < numMiners; i++ {
		m, err2 := CreateMiner(addr2)
		if err2 != nil {
			t.Errorf("Received error %v when creating miner", err2)
		}
		miners2[i] = m
	}

	client := CreateClient([]string{addr1, addr2})
	data := "data"
	upperbound := uint64(5000000)
	nonces, err := client.Mine(data, upperbound)
	if err != nil {
		t.Errorf("Received error %v when mining", err)
	} else {
		//client.Connect({"0"})
		for _, nonce := range nonces {
			expected := int64(2042628)
			if nonce != expected {
				t.Errorf("Expected nonce %d, but received %d", expected, nonce)
			}
		}
	}

}

func TestPools(t *testing.T) { //connect 2 clients to the same pool
	p, err := CreatePool("")
	if err != nil {
		t.Errorf("Received error %v when creating pool", err)
	}

	addr := p.Addr.String()

	numMiners := 2
	miners := make([]*Miner, numMiners)
	for i := 0; i < numMiners; i++ {
		m, err := CreateMiner(addr)
		if err != nil {
			t.Errorf("Received error %v when creating miner", err)
		}
		miners[i] = m
	}

	client1 := CreateClient([]string{addr})
	client2 := CreateClient([]string{addr})

	data := "data"
	upperbound := uint64(5000000)

	nonces1, err1 := client1.Mine(data, upperbound)
	if err1 != nil {
		t.Errorf("Received error %v when mining", err1)
	} else {
		for _, nonce1 := range nonces1 {
			expected1 := int64(2042628)
			if nonce1 != expected1 {
				t.Errorf("Expected nonce %d, but received %d", expected1, nonce1)
			}
		}
	}

	nonces2, err2 := client2.Mine(data, upperbound)
	if err2 != nil {
		t.Errorf("Received error %v when mining", err2)
	} else {
		for _, nonce2 := range nonces2 {
			expected2 := int64(2042628)
			if nonce2 != expected2 {
				t.Errorf("Expected nonce %d, but received %d", expected2, nonce2)
			}
		}
	}
}
