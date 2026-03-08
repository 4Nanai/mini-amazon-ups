package worldups

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"mini-amazon-ups/proto"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protodelim"
	protobuf "google.golang.org/protobuf/proto"
)

// UpsWorldClient handles communication between UPS service and World simulator
type UpsWorldClient struct {
	conn            net.Conn
	reader          *bufio.Reader
	worldID         int64
	seqNum          int64
	seqNumMutex     sync.Mutex
	responseChan    chan *proto.UResponses
	stopChan        chan struct{}
	connected       bool
	connMutex       sync.RWMutex
	pendingCommands map[int64]*pendingCommand
	pendingMutex    sync.Mutex

	// history of received sequence numbers for idempotency control
	historySeqs       map[int64]time.Time
	historyMutex      sync.RWMutex
	retentionDuration time.Duration
	lastCleanup       time.Time

	// reconnection state
	worldAddr    string
	savedWorldID *int64
	savedTrucks  []*proto.UInitTruck
	loopsStarted bool
}

type pendingCommand struct {
	msg        *proto.UCommands
	retryCount int
	nextRetry  time.Time
}

// NewUpsWorldClient creates a new UPS World client
func NewUpsWorldClient(worldAddr string) (*UpsWorldClient, error) {
	conn, err := net.Dial("tcp", worldAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to world: %w", err)
	}

	client := &UpsWorldClient{
		conn:              conn,
		reader:            bufio.NewReader(conn),
		seqNum:            0,
		responseChan:      make(chan *proto.UResponses, 100),
		stopChan:          make(chan struct{}),
		connected:         false,
		pendingCommands:   make(map[int64]*pendingCommand, 100),
		historySeqs:       make(map[int64]time.Time, 1000),
		retentionDuration: 5 * time.Minute,
		lastCleanup:       time.Now(),
		worldAddr:         worldAddr,
	}

	return client, nil
}

// Connect establishes connection with World simulator with auto-retry
func (c *UpsWorldClient) Connect(worldID *int64, trucks []*proto.UInitTruck) (int64, error) {
	// save params for potential reconnects
	c.savedWorldID = worldID
	c.savedTrucks = trucks

	backoff := time.Second
	maxBackoff := 32 * time.Second

	for {
		select {
		case <-c.stopChan:
			return 0, fmt.Errorf("connection stopped")
		default:
		}

		// Try to dial if we don't have a connection
		c.connMutex.RLock()
		hasConn := c.conn != nil
		c.connMutex.RUnlock()

		if !hasConn {
			slog.Info("Dialing World", "addr", c.worldAddr)
			conn, err := net.Dial("tcp", c.worldAddr)
			if err != nil {
				slog.Error("Failed to dial World, retrying", "error", err, "backoff", backoff)
				time.Sleep(backoff)
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
			c.connMutex.Lock()
			c.conn = conn
			c.reader = bufio.NewReader(conn)
			c.connMutex.Unlock()
		}

		// Prepare connect message
		isAmazon := false
		connectMsg := &proto.UConnect{
			IsAmazon: &isAmazon,
			Trucks:   trucks,
		}

		if worldID != nil {
			connectMsg.Worldid = worldID
		}

		// Send connect message
		if err := c.sendMessage(connectMsg); err != nil {
			slog.Error("Failed to send connect message, retrying", "error", err, "backoff", backoff)
			c.closeConnection()
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		// Wait for UConnected response
		connectedMsg := &proto.UConnected{}
		if err := c.receiveMessage(connectedMsg); err != nil {
			slog.Error("Failed to receive connected message, retrying", "error", err, "backoff", backoff)
			c.closeConnection()
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		// Success!
		c.worldID = connectedMsg.GetWorldid()
		c.connMutex.Lock()
		c.connected = true
		c.connMutex.Unlock()

		slog.Info("Connected to World", "worldID", c.worldID, "result", connectedMsg.GetResult())

		// Always start a receive loop for this connection
		go c.receiveLoop()
		// Start periodic loops only once
		if !c.loopsStarted {
			go c.retryLoop()
			go c.cleanupHistoryLoop()
			c.loopsStarted = true
		}

		return c.worldID, nil
	}
}

func NewUpsWorldClientAndConnect(worldAddr string, targetWorldID *int64, trucks []*proto.UInitTruck) (*UpsWorldClient, int64, error) {
	worldClient, err := NewUpsWorldClient(worldAddr)
	if err != nil {
		return nil, 0, err
	}
	var initTrucks []*proto.UInitTruck
	if trucks != nil {
		initTrucks = trucks
	} else {
		initTrucks = make([]*proto.UInitTruck, 0, 10)
		for i := range 10 {
			initTrucks = append(initTrucks, &proto.UInitTruck{
				Id: protobuf.Int32(int32(i + 1)),
				X:  protobuf.Int32(int32(i * 10)),
				Y:  protobuf.Int32(int32(i * 10)),
			})
		}
	}
	worldID, err := worldClient.Connect(targetWorldID, initTrucks)
	if err != nil {
		return nil, 0, err
	}
	slog.Info("[UPS] Connected to world simulator with world ID " + strconv.FormatInt(worldID, 10))
	return worldClient, worldID, nil
}

// GetNextSeqNum generates and returns the next sequence number
func (c *UpsWorldClient) GetNextSeqNum() int64 {
	c.seqNumMutex.Lock()
	defer c.seqNumMutex.Unlock()
	c.seqNum++
	return c.seqNum
}

// SendCommands sends commands to World simulator
func (c *UpsWorldClient) SendCommands(commands *proto.UCommands) error {
	c.connMutex.RLock()
	if !c.connected {
		c.connMutex.RUnlock()
		return fmt.Errorf("not connected to world")
	}
	c.connMutex.RUnlock()

	slog.Debug("Sending commands to World", "commands", commands)
	return c.sendMessage(commands)
}

// ReceiveResponses returns a channel for receiving responses from World
func (c *UpsWorldClient) ReceiveResponses() <-chan *proto.UResponses {
	return c.responseChan
}

// RequestPickup requests a truck to go to warehouse for pickup
func (c *UpsWorldClient) RequestPickup(truckID int32, whID int32) (int64, error) {
	seqNum := c.GetNextSeqNum()

	commands := &proto.UCommands{
		Pickups: []*proto.UGoPickup{
			{
				Truckid: &truckID,
				Whid:    &whID,
				Seqnum:  &seqNum,
			},
		},
	}

	// Add to pending commands for retry logic
	c.addPendingCommand(seqNum, commands)

	return seqNum, c.SendCommands(commands)
}

// RequestDelivery requests a truck to deliver packages to locations
func (c *UpsWorldClient) RequestDelivery(truckID int32, packages []*proto.UDeliveryLocation) (int64, error) {
	seqNum := c.GetNextSeqNum()

	commands := &proto.UCommands{
		Deliveries: []*proto.UGoDeliver{
			{
				Truckid:  &truckID,
				Packages: packages,
				Seqnum:   &seqNum,
			},
		},
	}

	// Add to pending commands for retry logic
	c.addPendingCommand(seqNum, commands)

	return seqNum, c.SendCommands(commands)
}

// QueryTruck queries the status of a truck
func (c *UpsWorldClient) QueryTruck(truckID int32) (int64, error) {
	seqNum := c.GetNextSeqNum()

	commands := &proto.UCommands{
		Queries: []*proto.UQuery{
			{
				Truckid: &truckID,
				Seqnum:  &seqNum,
			},
		},
	}

	// Add to pending commands for retry logic
	c.addPendingCommand(seqNum, commands)

	return seqNum, c.SendCommands(commands)
}

// SendAck sends acknowledgment for received messages
func (c *UpsWorldClient) SendAck(seqNums []int64) error {
	commands := &proto.UCommands{
		Acks: seqNums,
	}

	return c.SendCommands(commands)
}

// SetSimSpeed sets the simulation speed
func (c *UpsWorldClient) SetSimSpeed(speed uint32) error {
	commands := &proto.UCommands{
		Simspeed: &speed,
	}

	return c.SendCommands(commands)
}

// Disconnect closes the connection to World
func (c *UpsWorldClient) Disconnect() error {
	c.connMutex.Lock()
	if !c.connected {
		c.connMutex.Unlock()
		return nil
	}
	c.connected = false
	c.connMutex.Unlock()

	// Send disconnect command
	disconnect := true
	commands := &proto.UCommands{
		Disconnect: &disconnect,
	}
	c.sendMessage(commands)

	// Close channels and connection
	close(c.stopChan)
	time.Sleep(100 * time.Millisecond) // Give time for graceful shutdown

	if c.conn != nil {
		return c.conn.Close()
	}

	return nil
}

// receiveLoop continuously receives responses from World
func (c *UpsWorldClient) receiveLoop() {
	for {
		select {
		case <-c.stopChan:
			close(c.responseChan)
			return
		default:
			responses := &proto.UResponses{}
			if err := c.receiveMessage(responses); err != nil {
				if err != io.EOF {
					slog.Error("Failed to receive response", "error", err)
				}
				c.connMutex.Lock()
				c.connected = false
				c.connMutex.Unlock()

				// Attempt reconnection
				go c.attemptReconnect()
				return
			}

			// Handle ACKs - remove acknowledged commands from pending
			c.pendingMutex.Lock()
			for _, seqNum := range responses.GetAcks() {
				delete(c.pendingCommands, seqNum)
				slog.Debug("Received acknowledgment for command", "seqNum", seqNum)
			}
			c.pendingMutex.Unlock()

			// Filter out duplicate messages and create a new response with only new messages
			filteredResponses := &proto.UResponses{}
			processedSeqs := make([]int64, 0)

			// Filter Completions messages
			for _, completion := range responses.GetCompletions() {
				seqNum := completion.GetSeqnum()
				if c.isProcessed(seqNum) {
					processedSeqs = append(processedSeqs, seqNum)
					continue
				}
				c.markProcessed(seqNum)
				filteredResponses.Completions = append(filteredResponses.Completions, completion)
			}

			// Filter Delivered messages
			for _, delivered := range responses.GetDelivered() {
				seqNum := delivered.GetSeqnum()
				if c.isProcessed(seqNum) {
					processedSeqs = append(processedSeqs, seqNum)
					continue
				}
				c.markProcessed(seqNum)
				filteredResponses.Delivered = append(filteredResponses.Delivered, delivered)
			}

			// Filter TruckStatus messages
			for _, truckStatus := range responses.GetTruckstatus() {
				seqNum := truckStatus.GetSeqnum()
				if c.isProcessed(seqNum) {
					processedSeqs = append(processedSeqs, seqNum)
					continue
				}
				c.markProcessed(seqNum)
				filteredResponses.Truckstatus = append(filteredResponses.Truckstatus, truckStatus)
			}

			// Filter Error messages
			for _, errResp := range responses.GetError() {
				seqNum := errResp.GetSeqnum()
				if c.isProcessed(seqNum) {
					processedSeqs = append(processedSeqs, seqNum)
					continue
				}
				c.markProcessed(seqNum)
				filteredResponses.Error = append(filteredResponses.Error, errResp)
			}

			// Send ACK for duplicate messages
			if len(processedSeqs) > 0 {
				slog.Warn("Sending ACK for duplicate messages", "seqnums", processedSeqs)
				c.SendAck(processedSeqs)
			}

			// Only send to channel if there are new messages
			if len(filteredResponses.Completions) > 0 ||
				len(filteredResponses.Delivered) > 0 ||
				len(filteredResponses.Truckstatus) > 0 ||
				len(filteredResponses.Error) > 0 {
				select {
				case c.responseChan <- filteredResponses:
				case <-c.stopChan:
					close(c.responseChan)
					return
				}
			}
		}
	}
}

// sendMessage sends a protobuf message to World
func (c *UpsWorldClient) sendMessage(msg protobuf.Message) error {
	_, err := protodelim.MarshalTo(c.conn, msg)
	return err
}

// receiveMessage receives a protobuf message from World
func (c *UpsWorldClient) receiveMessage(msg protobuf.Message) error {
	return protodelim.UnmarshalFrom(c.reader, msg)
}

// IsConnected returns whether the client is connected to World
func (c *UpsWorldClient) IsConnected() bool {
	c.connMutex.RLock()
	defer c.connMutex.RUnlock()
	return c.connected
}

// GetWorldID returns the world ID
func (c *UpsWorldClient) GetWorldID() int64 {
	return c.worldID
}

func (c *UpsWorldClient) isProcessed(seqNum int64) bool {
	c.historyMutex.RLock()
	_, exists := c.historySeqs[seqNum]
	c.historyMutex.RUnlock()
	return exists
}

func (c *UpsWorldClient) markProcessed(seqNum int64) {
	c.historyMutex.Lock()
	c.historySeqs[seqNum] = time.Now()
	c.historyMutex.Unlock()
}

// attemptReconnect tries to re-establish the connection
func (c *UpsWorldClient) attemptReconnect() {
	slog.Info("Starting reconnection process")
	// Connect will handle retries automatically with exponential backoff
	_, err := c.Connect(c.savedWorldID, c.savedTrucks)
	if err != nil {
		// This should only happen if stopChan is closed
		slog.Error("Reconnection stopped", "error", err)
	}
}

// retryLoop continuously calls processPendingCommands to check for commands that need to be retried
func (c *UpsWorldClient) retryLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			return
		case <-ticker.C:
			c.processPendingCommands()
		}
	}
}

// processPendingCommands checks for pending commands and retries them if they have timed out
func (c *UpsWorldClient) processPendingCommands() {
	c.connMutex.RLock()
	if !c.connected {
		c.connMutex.RUnlock()
		return
	}
	c.connMutex.RUnlock()

	c.pendingMutex.Lock()
	defer c.pendingMutex.Unlock()

	now := time.Now()
	for seqNum, cmd := range c.pendingCommands {
		if now.After(cmd.nextRetry) {
			cmd.retryCount++
			backoff := time.Duration(2<<uint(cmd.retryCount)) * time.Second
			cmd.nextRetry = now.Add(backoff)
			go c.sendMessage(cmd.msg)
			slog.Debug("Retrying command", "seqnum", seqNum, "retryCount", cmd.retryCount, "backoff", backoff)

			if cmd.retryCount > 4 {
				slog.Error("Command failed after maximum retries", "seqnum", seqNum)
				delete(c.pendingCommands, seqNum)
			}
		}
	}
}

// addPendingCommand adds a command to the pending commands map for retry logic
func (c *UpsWorldClient) addPendingCommand(seqNum int64, msg *proto.UCommands) {
	c.pendingMutex.Lock()
	defer c.pendingMutex.Unlock()

	c.pendingCommands[seqNum] = &pendingCommand{
		msg:        msg,
		nextRetry:  time.Now().Add(time.Second),
		retryCount: 0,
	}
}

// cleanupHistoryLoop periodically calls cleanupHistory to remove old sequence numbers
func (c *UpsWorldClient) cleanupHistoryLoop() {
	ticker := time.NewTicker(c.retentionDuration)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			return
		case <-ticker.C:
			c.cleanupHistory()
		}
	}
}

// cleanupHistory removes old sequence numbers from the history to prevent unbounded growth
func (c *UpsWorldClient) cleanupHistory() {
	c.connMutex.RLock()
	if !c.connected {
		c.connMutex.RUnlock()
		return
	}
	c.connMutex.RUnlock()

	now := time.Now()
	c.historyMutex.Lock()
	for seqNum, timestamp := range c.historySeqs {
		if timestamp.Before(c.lastCleanup) {
			delete(c.historySeqs, seqNum)
		}
	}
	c.historyMutex.Unlock()
	c.lastCleanup = now
}

// closeConnection safely closes the current connection
func (c *UpsWorldClient) closeConnection() {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.reader = nil
	}
}
