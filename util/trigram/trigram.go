package trigram

import (
	"encoding/json"
	"fmt"
	"hash"

	"github.com/spaolacci/murmur3"
	"github.com/wkalt/dp3/util/bitset"
)

type Signature struct {
	Bitset bitset.Bitset
	Hash32 hash.Hash32
}

func (s Signature) MarshalJSON() ([]byte, error) {
	bytes := s.Bitset.Serialize()
	result, err := json.Marshal(bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal bitset to JSON: %w", err)
	}
	return result, nil
}

func (s *Signature) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &s.Bitset); err != nil {
		return fmt.Errorf("failed to unmarshal bitset: %w", err)
	}
	s.Hash32 = murmur3.New32()
	return nil
}

func NewSignature(sizeBytes int) Signature {
	return Signature{
		Bitset: bitset.New(sizeBytes),
		Hash32: murmur3.New32(),
	}
}

func (s Signature) AddTrigram(trgm string) {
	s.Hash32.Reset()
	_, _ = s.Hash32.Write([]byte(trgm))
	s.Bitset.SetBit(int(s.Hash32.Sum32()))
}

func (s Signature) AddString(text string) {
	for _, t := range ComputeTrigrams(text) {
		s.AddTrigram(t)
	}
}

func (s Signature) Contains(other Signature) bool {
	return s.Bitset.Contains(other.Bitset)
}

func (s Signature) Add(other Signature) {
	s.Bitset.Union(other.Bitset)
}

func ComputeTrigrams(text string) []string {
	result := []string{}
	n := len(text)
	if n == 0 {
		return result
	}

	// Add padding
	text = "  " + text + " "
	n += 3
	for i := 0; i < n-2; i++ {
		result = append(result, text[i:i+3])
	}
	return result
}
