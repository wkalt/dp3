package mcap

import (
	"fmt"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/wkalt/dp3/util"
)

func (c *MergeCoordinator) WriteMetadata(metadata *mcap.Metadata) error {
	if err := c.w.WriteMetadata(metadata); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}
	return nil
}

func (c *MergeCoordinator) handleSchema(skeleton bool, schema *mcap.Schema) (uint16, error) {
	var schemaID uint16
	// check if we have a matching schema by hash
	schemaHash := hashSchema(schema)
	if mappedID, ok := c.schemaHashes[schemaHash]; ok {
		// associate this schemau with the mapped ID
		c.schemas[schema] = mappedID
		schemaID = mappedID
	} else {
		schemaID = c.nextSchemaID
		newSchema := NewSchema(
			schemaID,
			schema.Name,
			schema.Encoding,
			util.When(!skeleton, schema.Data, []byte{}),
		)
		if err := c.w.WriteSchema(newSchema); err != nil {
			return schemaID, fmt.Errorf("failed to write schema: %w", err)
		}
		c.schemaHashes[schemaHash] = schemaID
		c.schemas[schema] = schemaID
		c.nextSchemaID++
	}
	return schemaID, nil
}

func (c *MergeCoordinator) handleChannel(schemaID uint16, channel *mcap.Channel) (uint16, error) {
	var chanID uint16
	// check if we have a matching channel by hash
	channelHash := hashChannel(channel)
	if mappedID, ok := c.channelHashes[channelHash]; ok {
		// associate this channel with the mapped ID
		c.channels[channel] = mappedID
		chanID = mappedID
	} else {
		newChannel := NewChannel(
			c.nextChannelID,
			schemaID,
			channel.Topic,
			channel.MessageEncoding,
			channel.Metadata,
		)
		if err := c.w.WriteChannel(newChannel); err != nil {
			return chanID, fmt.Errorf("failed to write channel: %w", err)
		}
		c.channels[channel] = c.nextChannelID
		c.channelHashes[channelHash] = c.nextChannelID
		chanID = c.nextChannelID
		c.nextChannelID++
	}
	return chanID, nil
}

func (c *MergeCoordinator) Write(
	schema *mcap.Schema,
	channel *mcap.Channel,
	msg *mcap.Message,
	skeleton bool,
) error {
	var err error
	schemaID, ok := c.schemas[schema]
	if !ok {
		if schemaID, err = c.handleSchema(skeleton, schema); err != nil {
			return fmt.Errorf("failed to handle schema: %w", err)
		}
	}
	chanID, ok := c.channels[channel]
	if !ok {
		if chanID, err = c.handleChannel(schemaID, channel); err != nil {
			return fmt.Errorf("failed to handle channel: %w", err)
		}
	}
	msg.ChannelID = chanID
	if skeleton {
		msg.Data = []byte{}
	}
	if err := c.w.WriteMessage(msg); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}
	return nil
}

func (c *MergeCoordinator) Close() error {
	if err := c.w.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return nil
}

type MergeCoordinator struct {
	w             *mcap.Writer
	schemaHashes  map[uint64]uint16
	channelHashes map[uint64]uint16
	schemas       map[*mcap.Schema]uint16
	channels      map[*mcap.Channel]uint16
	nextSchemaID  uint16
	nextChannelID uint16
}

func NewMergeCoordinator(w *mcap.Writer) *MergeCoordinator {
	return &MergeCoordinator{
		w:             w,
		schemaHashes:  make(map[uint64]uint16),
		channelHashes: make(map[uint64]uint16),
		schemas:       make(map[*mcap.Schema]uint16),
		channels:      make(map[*mcap.Channel]uint16),

		nextSchemaID: 1,
	}
}
