package bitset

type Bitset []byte

func New(sizeBytes int) Bitset {
	return make([]byte, sizeBytes)
}

func (b Bitset) SetBit(i int) {
	bitidx := i % (len(b) * 8)
	b[bitidx/8] |= 1 << (bitidx % 8)
}

func (b Bitset) HasBit(i int) bool {
	bitidx := i % (len(b) * 8)
	return b[bitidx/8]&(1<<(bitidx%8)) != 0
}

func (b Bitset) Contains(other Bitset) bool {
	for i, v := range other {
		if b[i]&v != v {
			return false
		}
	}
	return true
}

func (b Bitset) Add(other Bitset) {
	for i, v := range other {
		b[i] |= v
	}
}

func (b Bitset) Serialize() []byte {
	return b
}
