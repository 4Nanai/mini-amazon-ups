package worldups

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"mini-amazon-ups/proto"
	"net"
	"sync"
	"time"

	protobuf "google.golang.org/protobuf/proto"
)

// UpsWorldClient handles communication between UPS service and World simulator
type UpsWorldClient struct {
	conn         net.Conn
	worldID      int64
	seqNum       int64
	seqNumMutex  sync.Mutex
	responseChan chan *proto.UResponses
	stopChan     chan struct{}
	connected    bool
	connMutex    sync.RWMutex
}

// NewUpsWorldClient creates a new UPS World client
func NewUpsWorldClient(worldAddr string) (*UpsWorldClient, error) {
	conn, err := net.Dial("tcp", worldAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to world: %w", err)
	}

	client := &UpsWorldClient{
		conn:         conn,
		seqNum:       0,
		responseChan: make(chan *proto.UResponses, 100),
		stopChan:     make(chan struct{}),
		connected:    false,
	}

	return client, nil
}

// Connect establishes connection with World simulator
func (c *UpsWorldClient) Connect(worldID *int64, trucks []*proto.UInitTruck) (int64, error) {
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
		return 0, fmt.Errorf("failed to send connect message: %w", err)
	}

	// Wait for UConnected response
	connectedMsg := &proto.UConnected{}
	if err := c.receiveMessage(connectedMsg); err != nil {
		return 0, fmt.Errorf("failed to receive connected message: %w", err)
	}

	c.worldID = connectedMsg.GetWorldid()
	c.connMutex.Lock()
	c.connected = true
	c.connMutex.Unlock()

	slog.Info("Connected to World", "worldID", c.worldID, "result", connectedMsg.GetResult())

	// Start receiving responses
	go c.receiveLoop()

	return c.worldID, nil
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
				close(c.responseChan)
				return
			}

			select {
			case c.responseChan <- responses:
			case <-c.stopChan:
				close(c.responseChan)
				return
			}
		}
	}
}

// sendMessage sends a protobuf message to World
func (c *UpsWorldClient) sendMessage(msg protobuf.Message) error {
	data, err := protobuf.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Send message length (4 bytes, big-endian)
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(len(data)))

	if _, err := c.conn.Write(lengthBuf); err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}

	// Send message data
	if _, err := c.conn.Write(data); err != nil {
		return fmt.Errorf("failed to write message data: %w", err)
	}

	return nil
}

// receiveMessage receives a protobuf message from World
func (c *UpsWorldClient) receiveMessage(msg protobuf.Message) error {
	// Read message length (4 bytes, big-endian)
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, lengthBuf); err != nil {
		return err
	}

	length := binary.BigEndian.Uint32(lengthBuf)

	// Read message data
	data := make([]byte, length)
	if _, err := io.ReadFull(c.conn, data); err != nil {
		return err
	}

	// Unmarshal message
	if err := protobuf.Unmarshal(data, msg); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return nil
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
