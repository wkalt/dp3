package bitset

type Bitset []byte

func New(sizeBytes int) Bitset {
	return make([]byte, sizeBytes)
}

func (b Bitset) SetBit(i int) {
	m := i % len(b)
	b[m/8] |= 1 << (m % 8)
}

func (b Bitset) HasBit(i int) bool {
	m := i % len(b)
	return b[m/8]&(1<<(m%8)) != 0
}

func (b Bitset) Contains(other Bitset) bool {
	for i, v := range other {
		if b[i]&v != v {
			return false
		}
	}
	return true
}

func (b Bitset) Union(other Bitset) {
	for i, v := range other {
		b[i] |= v
	}
}

func (b Bitset) Serialize() []byte {
	return b
}
