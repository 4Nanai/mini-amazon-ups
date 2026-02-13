package worldamazon

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

// AmazonWorldClient handles communication between Amazon service and World simulator
type AmazonWorldClient struct {
	conn         net.Conn
	worldID      int64
	seqNum       int64
	seqNumMutex  sync.Mutex
	responseChan chan *proto.AResponses
	stopChan     chan struct{}
	connected    bool
	connMutex    sync.RWMutex
}

// NewAmazonWorldClient creates a new Amazon World client
func NewAmazonWorldClient(worldAddr string) (*AmazonWorldClient, error) {
	conn, err := net.Dial("tcp", worldAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to world: %w", err)
	}

	client := &AmazonWorldClient{
		conn:         conn,
		seqNum:       0,
		responseChan: make(chan *proto.AResponses, 100),
		stopChan:     make(chan struct{}),
		connected:    false,
	}

	return client, nil
}

// Connect establishes connection with World simulator
func (c *AmazonWorldClient) Connect(worldID *int64, warehouses []*proto.AInitWarehouse) (int64, error) {
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
		return 0, fmt.Errorf("failed to send connect message: %w", err)
	}

	// Wait for AConnected response
	connectedMsg := &proto.AConnected{}
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
func (c *AmazonWorldClient) sendMessage(msg protobuf.Message) error {
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
func (c *AmazonWorldClient) receiveMessage(msg protobuf.Message) error {
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
func (c *AmazonWorldClient) IsConnected() bool {
	c.connMutex.RLock()
	defer c.connMutex.RUnlock()
	return c.connected
}

// GetWorldID returns the world ID
func (c *AmazonWorldClient) GetWorldID() int64 {
	return c.worldID
}
