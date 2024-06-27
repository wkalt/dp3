package nodestore

import (
	"errors"

	"github.com/wkalt/dp3/server/util"
)

type MessageKey struct {
	Timestamp uint64
	Sequence  uint32
}

func serializeKeys(keys []MessageKey) []byte {
	serialized := make([]byte, len(keys)*12)
	offset := 0
	for _, key := range keys {
		offset += util.U64(serialized[offset:], key.Timestamp)
		offset += util.U32(serialized[offset:], key.Sequence)
	}
	return serialized
}

func deserializeKeys(data []byte) ([]MessageKey, error) {
	offset := 0
	if len(data)%12 != 0 {
		return nil, errors.New("invalid data length")
	}
	keys := make([]MessageKey, len(data)/12)
	for i := range keys {
		offset += util.ReadU64(data[offset:], &keys[i].Timestamp)
		offset += util.ReadU32(data[offset:], &keys[i].Sequence)
	}
	return keys, nil
}

func NewMessageKey(timestamp uint64, sequence uint32) MessageKey {
	return MessageKey{Timestamp: timestamp, Sequence: sequence}
}
