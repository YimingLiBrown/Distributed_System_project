/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: a LiteMiner mining pool.
 */

package liteminer

import (
	"encoding/gob"
	"io"
	"net"
	"sync"
	"time"
)

const HEARTBEAT_TIMEOUT = 3 * HEARTBEAT_FREQ

// Represents a LiteMiner mining pool
type Pool struct {
	Addr     net.Addr                // Address of the pool
	Miners   map[net.Addr]MiningConn // Currently connected miners
	Client   MiningConn              // The current client
	busy     bool                    // True when processing a transaction
	mutex    sync.Mutex              // To manage concurrent access to these members
	Intv     []Interval
	Data     string
	Results  []Message
	WorkChan chan Interval // Channel for work to be done
}

// CreatePool creates a new pool at the specified port.
func CreatePool(port string) (pp *Pool, err error) {
	var pool Pool

	pp = &pool

	pool.busy = false
	pool.Client.Conn = nil
	pool.Miners = make(map[net.Addr]MiningConn)

	// TODO: Students should (if necessary) initialize any additional members
	// to the Pool struct here.
	pp.WorkChan = make(chan Interval)
	err = pp.startListener(port)

	return
}

// startListener starts listening for new connections.
func (p *Pool) startListener(port string) (err error) {
	listener, portId, err := OpenListener(port)
	if err != nil {
		return
	}

	p.Addr = listener.Addr()

	Out.Printf("Listening on port %v\n", portId)

	// Listen for and accept connections
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				Err.Printf("Received error %v when listening for connections\n", err)
				continue
			}

			go p.handleConnection(conn)
		}
	}()

	return
}

// handleConnection handles an incoming connection and delegates to
// handleMinerConnection or handleClientConnection.
func (p *Pool) handleConnection(nc net.Conn) {
	// Set up connection
	conn := MiningConn{}
	conn.Conn = nc
	conn.Enc = gob.NewEncoder(nc)
	conn.Dec = gob.NewDecoder(nc)

	// Wait for Hello message
	msg, err := RecvMsg(conn)
	if err != nil {
		Err.Printf(
			"Received error %v when processing Hello message from %v\n",
			err,
			conn.Conn.RemoteAddr(),
		)
		conn.Conn.Close() // Close the connection
		return
	}

	switch msg.Type {
	case MinerHello:
		p.handleMinerConnection(conn)
	case ClientHello:
		p.handleClientConnection(conn)
	default:
		SendMsg(conn, ErrorMsg("Unexpected message type"))
	}
}

// handleClientConnection handles a connection from a client.
func (p *Pool) handleClientConnection(conn MiningConn) {
	Debug.Printf("Received client connection from %v", conn.Conn.RemoteAddr())

	p.mutex.Lock()
	if p.Client.Conn != nil {
		Debug.Printf(
			"Busy with client %v, sending BusyPool message to client %v",
			p.Client.Conn.RemoteAddr(),
			conn.Conn.RemoteAddr(),
		)
		SendMsg(conn, BusyPoolMsg())
		p.mutex.Unlock()
		return
	} else if p.busy {
		Debug.Printf(
			"Busy with previous transaction, sending BusyPool message to client %v",
			conn.Conn.RemoteAddr(),
		)
		SendMsg(conn, BusyPoolMsg())
		p.mutex.Unlock()
		return
	}
	p.Client = conn
	p.mutex.Unlock()

	// Listen for and handle incoming messages
	for {
		msg, err := RecvMsg(conn)
		if err != nil {
			if err == io.EOF {
				Out.Printf("Client %v disconnected\n", conn.Conn.RemoteAddr())

				conn.Conn.Close() // Close the connection

				p.mutex.Lock()
				p.Client.Conn = nil
				p.mutex.Unlock()

				return
			}
			Err.Printf(
				"Received error %v when processing message from client %v\n",
				err,
				conn.Conn.RemoteAddr(),
			)
			continue
		}

		if msg.Type != Transaction {
			SendMsg(conn, ErrorMsg("Expected Transaction message"))
			continue
		}

		Debug.Printf(
			"Received transaction from client %v with data %v and upper bound %v",
			conn.Conn.RemoteAddr(),
			msg.Data,
			msg.Upper,
		)

		// IF THERE ARE NO MINERS:
		p.mutex.Lock()
		if len(p.Miners) == 0 {
			SendMsg(conn, ErrorMsg("No miners connected"))
			p.mutex.Unlock()
			continue
		}

		p.busy = true
		p.Data = msg.Data
		multiplier := 150
		p.Intv = GenerateIntervals(msg.Upper, multiplier)

		p.mutex.Unlock()
		for _, interval := range p.Intv {
			p.WorkChan <- interval
		}

		continue
		// TODO: Students should handle an incoming transaction from a client. A
		// pool may process one transaction at a time – thus, if you receive
		// another transaction while busy, you should send a BusyPool message.
	}
}

// handleMinerConnection handles a connection from a miner.
func (p *Pool) handleMinerConnection(conn MiningConn) {
	Debug.Printf("Received miner connection from %v", conn.Conn.RemoteAddr())

	p.mutex.Lock()
	p.Miners[conn.Conn.RemoteAddr()] = conn
	p.mutex.Unlock()
	msgChan := make(chan Message)
	go p.receiveFromMiner(conn, msgChan)

	for work := range p.WorkChan {
		// ASSIGN JOB TO THIS CURRENT MINER
		timer := time.After(HEARTBEAT_TIMEOUT)
		updateCount := 0
		msgToSend := MineRequestMsg(p.Data, work.Lower, work.Upper)
		SendMsg(conn, msgToSend)
		isMining := true
		for isMining {
			select {
			case msg := <-msgChan:
				if msg.Type == ProofOfWork {
					p.mutex.Lock()
					p.Results = append(p.Results, msg)
					p.mutex.Unlock()
					if len(p.Results) == len(p.Intv) {
						// SEND BACK THE SMALLEST NUMBER.
						msgToReturn := p.Results[0]
						for _, number := range p.Results {
							if number.Hash < msgToReturn.Hash {
								msgToReturn = number
							}
						}
						SendMsg(p.Client, ProofOfWorkMsg(msgToReturn.Data, msgToReturn.Nonce, msgToReturn.Hash))
						// RESTART POOL HERE.
						p.mutex.Lock()
						p.Data = ""
						p.Results = make([]Message, 0)
						p.busy = false
						p.mutex.Unlock()
					}
					isMining = false
				} else if msg.Type == StatusUpdate {
					updateCount++
					timer = time.After(HEARTBEAT_TIMEOUT)
					if updateCount == 5 {
						p.WorkChan <- work
						p.mutex.Lock()
						Debug.Printf("SLOW MINER AT: %v send update.", conn.Conn.RemoteAddr())
						delete(p.Miners, conn.Conn.RemoteAddr())
						p.mutex.Unlock()
					}
				}
			case <-timer:
				p.WorkChan <- work
				p.mutex.Lock()
				delete(p.Miners, conn.Conn.RemoteAddr())
				p.mutex.Unlock()
			default:
				// Debug.Printf("sth went wrong, here is responsible for pool handling miner connection.")
			}
		}

	}
	// TODO: Students should handle a miner connection. If a miner does not
	// send a StatusUpdate message every HEARTBEAT_TIMEOUT while mining,
	// any work assigned to them should be redistributed and they should be
	// disconnected and removed from p.Miners.
}

// receiveFromMiner waits for messages from the miner specified by conn and
// forwards them over msgChan.
func (p *Pool) receiveFromMiner(conn MiningConn, msgChan chan Message) {
	for {
		msg, err := RecvMsg(conn)
		if err != nil {
			if _, ok := err.(*net.OpError); ok || err == io.EOF {
				Out.Printf("Miner %v disconnected\n", conn.Conn.RemoteAddr())

				p.mutex.Lock()
				delete(p.Miners, conn.Conn.RemoteAddr())
				p.mutex.Unlock()

				conn.Conn.Close() // Close the connection

				return
			}
			Err.Printf(
				"Received error %v when processing message from miner %v\n",
				err,
				conn.Conn.RemoteAddr(),
			)
			continue
		}
		msgChan <- msg
	}
}

// GetMiners returns the addresses of any connected miners.
func (p *Pool) GetMiners() []net.Addr {
	miners := []net.Addr{}
	p.mutex.Lock()
	for _, m := range p.Miners {
		miners = append(miners, m.Conn.RemoteAddr())
	}
	p.mutex.Unlock()
	return miners
}

// GetClient returns the address of the current client or nil if there is no
// current client.
func (p *Pool) GetClient() net.Addr {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.Client.Conn == nil {
		return nil
	}
	return p.Client.Conn.RemoteAddr()
}
