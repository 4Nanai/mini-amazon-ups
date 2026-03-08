package worldamazon

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

// AmazonWorldClient handles communication between Amazon service and World simulator
type AmazonWorldClient struct {
	conn            net.Conn
	reader          *bufio.Reader
	worldID         int64
	seqNum          int64
	seqNumMutex     sync.Mutex
	responseChan    chan *proto.AResponses
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
	loopsStarted bool
}

type pendingCommand struct {
	msg        *proto.ACommands
	retryCount int
	nextRetry  time.Time
}

// NewAmazonWorldClient creates a new Amazon World client
func NewAmazonWorldClient(worldAddr string) (*AmazonWorldClient, error) {
	conn, err := net.Dial("tcp", worldAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to world: %w", err)
	}

	client := &AmazonWorldClient{
		conn:              conn,
		reader:            bufio.NewReader(conn),
		seqNum:            0,
		responseChan:      make(chan *proto.AResponses, 100),
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

func NewAmazonWorldClientAndConnect(worldAddr string, targetWorldID *int64, warehouses []*proto.AInitWarehouse) (*AmazonWorldClient, int64, error) {
	worldClient, err := NewAmazonWorldClient(worldAddr)
	if err != nil {
		return nil, 0, err
	}
	var initWarehouses []*proto.AInitWarehouse
	if warehouses != nil {
		initWarehouses = warehouses
	} else {
		initWarehouses = make([]*proto.AInitWarehouse, 0, 10)
		for i := range 10 {
			initWarehouses = append(initWarehouses, &proto.AInitWarehouse{
				Id: protobuf.Int32(int32(i + 1)),
				X:  protobuf.Int32(int32(i * 10)),
				Y:  protobuf.Int32(int32(i * 10)),
			})
		}
	}
	worldID, err := worldClient.Connect(targetWorldID, initWarehouses)
	if err != nil {
		return nil, 0, err
	}
	slog.Info("[Amazon] Connected to world simulator with world ID " + strconv.FormatInt(worldID, 10))
	return worldClient, worldID, nil
}

// Connect establishes connection with World simulator with auto-retry
func (c *AmazonWorldClient) Connect(worldID *int64, warehouses []*proto.AInitWarehouse) (int64, error) {
	// save params for potential reconnects
	c.savedWorldID = worldID

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
				slog.Warn("Failed to dial World", "error", err, "nextBackoff", backoff)
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
		isAmazon := true
		connectMsg := &proto.AConnect{
			IsAmazon: &isAmazon,
			Initwh:   warehouses,
		}

		if worldID != nil {
			connectMsg.Worldid = worldID
		}

		// Send connect message
		if err := c.sendMessage(connectMsg); err != nil {
			slog.Warn("Failed to send connect message", "error", err, "nextBackoff", backoff)
			c.closeConnection()
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		// Wait for AConnected response
		connectedMsg := &proto.AConnected{}
		if err := c.receiveMessage(connectedMsg); err != nil {
			slog.Warn("Failed to receive connected message", "error", err, "nextBackoff", backoff)
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

// GetNextSeqNum generates and returns the next sequence number
func (c *AmazonWorldClient) GetNextSeqNum() int64 {
	c.seqNumMutex.Lock()
	defer c.seqNumMutex.Unlock()
	c.seqNum++
	return c.seqNum
}

// SendCommands sends commands to World simulator
func (c *AmazonWorldClient) SendCommands(commands *proto.ACommands) error {
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
func (c *AmazonWorldClient) ReceiveResponses() <-chan *proto.AResponses {
	return c.responseChan
}

// RequestPurchase requests to purchase more items at a warehouse
func (c *AmazonWorldClient) RequestPurchase(whnum int32, products []*proto.AProduct) (int64, error) {
	seqNum := c.GetNextSeqNum()

	commands := &proto.ACommands{
		Buy: []*proto.APurchaseMore{
			{
				Whnum:  &whnum,
				Things: products,
				Seqnum: &seqNum,
			},
		},
	}

	// Add to pending commands for retry logic
	c.addPendingCommand(seqNum, commands)

	return seqNum, c.SendCommands(commands)
}

// RequestPack requests to pack items for shipping
func (c *AmazonWorldClient) RequestPack(whnum int32, products []*proto.AProduct, shipID int64) (int64, error) {
	seqNum := c.GetNextSeqNum()

	commands := &proto.ACommands{
		Topack: []*proto.APack{
			{
				Whnum:  &whnum,
				Things: products,
				Shipid: &shipID,
				Seqnum: &seqNum,
			},
		},
	}

	// Add to pending commands for retry logic
	c.addPendingCommand(seqNum, commands)

	return seqNum, c.SendCommands(commands)
}

// RequestLoad requests to load a package onto a truck
func (c *AmazonWorldClient) RequestLoad(whnum int32, truckID int32, shipID int64) (int64, error) {
	seqNum := c.GetNextSeqNum()

	commands := &proto.ACommands{
		Load: []*proto.APutOnTruck{
			{
				Whnum:   &whnum,
				Truckid: &truckID,
				Shipid:  &shipID,
				Seqnum:  &seqNum,
			},
		},
	}

	// Add to pending commands for retry logic
	c.addPendingCommand(seqNum, commands)

	return seqNum, c.SendCommands(commands)
}

// QueryPackage queries the status of a package
func (c *AmazonWorldClient) QueryPackage(packageID int64) (int64, error) {
	seqNum := c.GetNextSeqNum()

	commands := &proto.ACommands{
		Queries: []*proto.AQuery{
			{
				Packageid: &packageID,
				Seqnum:    &seqNum,
			},
		},
	}

	// Add to pending commands for retry logic
	c.addPendingCommand(seqNum, commands)

	return seqNum, c.SendCommands(commands)
}

// SendAck sends acknowledgment for received messages
func (c *AmazonWorldClient) SendAck(seqNums []int64) error {
	commands := &proto.ACommands{
		Acks: seqNums,
	}

	return c.SendCommands(commands)
}

// SetSimSpeed sets the simulation speed
func (c *AmazonWorldClient) SetSimSpeed(speed uint32) error {
	commands := &proto.ACommands{
		Simspeed: &speed,
	}

	return c.SendCommands(commands)
}

// Disconnect closes the connection to World
func (c *AmazonWorldClient) Disconnect() error {
	c.connMutex.Lock()
	if !c.connected {
		c.connMutex.Unlock()
		return nil
	}
	c.connected = false
	c.connMutex.Unlock()

	// Send disconnect command
	disconnect := true
	commands := &proto.ACommands{
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
func (c *AmazonWorldClient) receiveLoop() {
	for {
		select {
		case <-c.stopChan:
			close(c.responseChan)
			return
		default:
			responses := &proto.AResponses{}
			if err := c.receiveMessage(responses); err != nil {
				if err != io.EOF {
					slog.Error("Failed to receive response", "error", err)
				}
				c.connMutex.Lock()
				c.connected = false
				c.connMutex.Unlock()
				// start reconnect attempts and return from this receiveLoop
				go c.attemptReconnect()
				return
			}

			c.pendingMutex.Lock()
			for _, seqNum := range responses.GetAcks() {
				delete(c.pendingCommands, seqNum)
				slog.Debug("Received acknowledgment for command", "seqNum", seqNum)
			}
			c.pendingMutex.Unlock()

			// Filter out duplicate messages and create a new response with only new messages
			filteredResponses := &proto.AResponses{}
			processedSeqs := make([]int64, 0)

			// Filter Arrived messages
			for _, arrived := range responses.GetArrived() {
				seqNum := arrived.GetSeqnum()
				if c.isProcessed(seqNum) {
					slog.Warn("Received duplicate Arrived message, ignoring", "seqNum", seqNum)
					processedSeqs = append(processedSeqs, seqNum)
				} else {
					c.markProcessed(seqNum)
					filteredResponses.Arrived = append(filteredResponses.Arrived, arrived)
				}
			}

			// Filter Loaded messages
			for _, loaded := range responses.GetLoaded() {
				seqNum := loaded.GetSeqnum()
				if c.isProcessed(seqNum) {
					slog.Warn("Received duplicate Loaded message, ignoring", "seqNum", seqNum)
					processedSeqs = append(processedSeqs, seqNum)
				} else {
					c.markProcessed(seqNum)
					filteredResponses.Loaded = append(filteredResponses.Loaded, loaded)
				}
			}

			// Filter PackageStatus messages
			for _, pkg := range responses.GetPackagestatus() {
				seqNum := pkg.GetSeqnum()
				if c.isProcessed(seqNum) {
					slog.Warn("Received duplicate PackageStatus message, ignoring", "seqNum", seqNum)
					processedSeqs = append(processedSeqs, seqNum)
				} else {
					c.markProcessed(seqNum)
					filteredResponses.Packagestatus = append(filteredResponses.Packagestatus, pkg)
				}
			}

			// Filter Ready messages
			for _, ready := range responses.GetReady() {
				seqNum := ready.GetSeqnum()
				if c.isProcessed(seqNum) {
					slog.Warn("Received duplicate Ready message, ignoring", "seqNum", seqNum)
					processedSeqs = append(processedSeqs, seqNum)
				} else {
					c.markProcessed(seqNum)
					filteredResponses.Ready = append(filteredResponses.Ready, ready)
				}
			}

			// Filter Error messages
			for _, errResp := range responses.GetError() {
				seqNum := errResp.GetSeqnum()
				if c.isProcessed(seqNum) {
					slog.Warn("Received duplicate Error message, ignoring", "seqNum", seqNum)
					processedSeqs = append(processedSeqs, seqNum)
				} else {
					c.markProcessed(seqNum)
					filteredResponses.Error = append(filteredResponses.Error, errResp)
				}
			}

			// Send ACK for duplicate messages
			if len(processedSeqs) > 0 {
				c.SendAck(processedSeqs)
			}

			// Only send to channel if there are new messages
			hasNewMessages := len(filteredResponses.GetArrived()) > 0 ||
				len(filteredResponses.GetLoaded()) > 0 ||
				len(filteredResponses.GetPackagestatus()) > 0 ||
				len(filteredResponses.GetReady()) > 0 ||
				len(filteredResponses.GetError()) > 0

			if hasNewMessages {
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

func (c *AmazonWorldClient) isProcessed(seqNum int64) bool {
	c.historyMutex.RLock()
	_, exists := c.historySeqs[seqNum]
	c.historyMutex.RUnlock()
	return exists
}

func (c *AmazonWorldClient) markProcessed(seqNum int64) {
	c.historyMutex.Lock()
	c.historySeqs[seqNum] = time.Now()
	c.historyMutex.Unlock()
}

// attemptReconnect tries to re-establish the connection
// Connect method now handles all retry logic automatically
func (c *AmazonWorldClient) attemptReconnect() {
	slog.Info("Starting reconnection process")
	// Connect will handle retries automatically with exponential backoff
	_, err := c.Connect(c.savedWorldID, nil)
	if err != nil {
		// This should only happen if stopChan is closed
		slog.Error("Reconnection stopped", "error", err)
	}
}

// retryLoop continuously calls processPendingCommands to check for commands that need to be retried
func (c *AmazonWorldClient) retryLoop() {
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
func (c *AmazonWorldClient) processPendingCommands() {
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
			slog.Debug("Retrying command", "seqNum", seqNum, "retryCount", cmd.retryCount, "backoff", backoff)

			if cmd.retryCount > 4 {
				slog.Error("Command failed after maximum retries", "seqNum", seqNum)
				delete(c.pendingCommands, seqNum)
			}
		}
	}
}

// addPendingCommand adds a command to the pending commands map for retry logic
func (c *AmazonWorldClient) addPendingCommand(seqNum int64, msg *proto.ACommands) {
	c.pendingMutex.Lock()
	defer c.pendingMutex.Unlock()

	c.pendingCommands[seqNum] = &pendingCommand{
		msg:        msg,
		nextRetry:  time.Now().Add(time.Second),
		retryCount: 0,
	}
}

// cleanupHistoryLoop periodically calls cleanupHistory to remove old sequence numbers
func (c *AmazonWorldClient) cleanupHistoryLoop() {
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
func (c *AmazonWorldClient) cleanupHistory() {
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
func (c *AmazonWorldClient) closeConnection() {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.reader = nil
	}
}

// sendMessage sends a protobuf message to World
func (c *AmazonWorldClient) sendMessage(msg protobuf.Message) error {
	_, err := protodelim.MarshalTo(c.conn, msg)
	return err
}

// receiveMessage receives a protobuf message from World
func (c *AmazonWorldClient) receiveMessage(msg protobuf.Message) error {
	return protodelim.UnmarshalFrom(c.reader, msg)
}

// IsConnected returns whether the client is connected to World
func (c *AmazonWorldClient) IsConnected() bool {
	c.connMutex.RLock()
	defer c.connMutex.RUnlock()
	return c.connected
}

// GetWorldID returns the world ID
func (c *AmazonWorldClient) GetWorldID() int64 {
	return c.worldID
}
