package core

import (
	"encoding/binary"
	"errors"
	"math/big"
	"math/bits"
)

var (
	ErrUint128Overflow      = errors.New("uint128 overflow")
	ErrUint128Underflow     = errors.New("uint128 underflow")
	ErrInvalidUint128Binary = errors.New("invalid uint128 binary length")
	ErrInvalidUint128String = errors.New("invalid uint128 decimal string")
)

// Uint128 stores an unsigned 128-bit integer as two uint64 words.
// Hi contains the most significant 64 bits, Lo the least significant 64 bits.
type Uint128 struct {
	Lo uint64
	Hi uint64
}

func Add(a, b Uint128) (Uint128, error) {
	lo, carry := bits.Add64(a.Lo, b.Lo, 0)
	hi, carryHi := bits.Add64(a.Hi, b.Hi, carry)
	if carryHi != 0 {
		return Uint128{}, ErrUint128Overflow
	}
	return Uint128{Lo: lo, Hi: hi}, nil
}

func Sub(a, b Uint128) (Uint128, error) {
	if Cmp(a, b) < 0 {
		return Uint128{}, ErrUint128Underflow
	}
	lo, borrow := bits.Sub64(a.Lo, b.Lo, 0)
	hi, borrowHi := bits.Sub64(a.Hi, b.Hi, borrow)
	if borrowHi != 0 {
		return Uint128{}, ErrUint128Underflow
	}
	return Uint128{Lo: lo, Hi: hi}, nil
}

func Cmp(a, b Uint128) int {
	if a.Hi < b.Hi {
		return -1
	}
	if a.Hi > b.Hi {
		return 1
	}
	if a.Lo < b.Lo {
		return -1
	}
	if a.Lo > b.Lo {
		return 1
	}
	return 0
}

func IsZero(a Uint128) bool {
	return a.Hi == 0 && a.Lo == 0
}

func FromUint64(v uint64) Uint128 {
	return Uint128{Lo: v}
}

func FromString(s string) (Uint128, error) {
	if s == "" {
		return Uint128{}, ErrInvalidUint128String
	}

	acc := Uint128{}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return Uint128{}, ErrInvalidUint128String
		}

		mul, err := mul64(acc, 10)
		if err != nil {
			return Uint128{}, err
		}
		acc = mul

		acc, err = Add(acc, FromUint64(uint64(c-'0')))
		if err != nil {
			return Uint128{}, err
		}
	}
	return acc, nil
}

func String(a Uint128) string {
	if IsZero(a) {
		return "0"
	}

	bi := new(big.Int)
	bi.SetBits([]big.Word{big.Word(a.Lo), big.Word(a.Hi)})
	return bi.String()
}

func MarshalBinary(a Uint128) []byte {
	out := make([]byte, 16)
	binary.BigEndian.PutUint64(out[:8], a.Hi)
	binary.BigEndian.PutUint64(out[8:], a.Lo)
	return out
}

func UnmarshalBinary(b []byte) (Uint128, error) {
	if len(b) != 16 {
		return Uint128{}, ErrInvalidUint128Binary
	}
	return Uint128{
		Hi: binary.BigEndian.Uint64(b[:8]),
		Lo: binary.BigEndian.Uint64(b[8:]),
	}, nil
}

func mul64(a Uint128, m uint64) (Uint128, error) {
	hiHi, hiLo := bits.Mul64(a.Hi, m)
	loHi, loLo := bits.Mul64(a.Lo, m)
	newHi, carry := bits.Add64(hiLo, loHi, 0)
	if hiHi != 0 || carry != 0 {
		return Uint128{}, ErrUint128Overflow
	}
	return Uint128{Hi: newHi, Lo: loLo}, nil
}
