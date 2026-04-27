package unit

import (
	"encoding/hex"
	"errors"
	"testing"
	"unsafe"

	"aequitas-ledger/internal/core"
)

func TestUint128AddOverflow(t *testing.T) {
	a := core.Uint128{Hi: ^uint64(0), Lo: ^uint64(0)}
	b := core.FromUint64(1)

	_, err := core.Add(a, b)
	if !errors.Is(err, core.ErrUint128Overflow) {
		t.Fatalf("expected overflow, got %v", err)
	}
}

func TestUint128SubUnderflow(t *testing.T) {
	a := core.FromUint64(1)
	b := core.FromUint64(2)

	_, err := core.Sub(a, b)
	if !errors.Is(err, core.ErrUint128Underflow) {
		t.Fatalf("expected underflow, got %v", err)
	}
}

func TestUint128BinaryRoundTrip(t *testing.T) {
	in := core.Uint128{Hi: 0x0123456789abcdef, Lo: 0xfedcba9876543210}

	enc := core.MarshalBinary(in)
	if got := hex.EncodeToString(enc); got != "0123456789abcdeffedcba9876543210" {
		t.Fatalf("unexpected encoding: %s", got)
	}

	out, err := core.UnmarshalBinary(enc)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if core.Cmp(in, out) != 0 {
		t.Fatalf("round-trip mismatch: in=%+v out=%+v", in, out)
	}
}

func TestUint128StringRoundTrip(t *testing.T) {
	cases := []string{
		"0",
		"1",
		"18446744073709551615",
		"340282366920938463463374607431768211455", // max uint128
	}

	for _, tc := range cases {
		t.Run(tc, func(t *testing.T) {
			u, err := core.FromString(tc)
			if err != nil {
				t.Fatalf("FromString(%q) error: %v", tc, err)
			}
			if got := core.String(u); got != tc {
				t.Fatalf("string round-trip mismatch: got=%s want=%s", got, tc)
			}
		})
	}
}

func FuzzUint128StringRoundTrip(f *testing.F) {
	seeds := []string{"0", "1", "42", "999999999999999999", "340282366920938463463374607431768211455"}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		u, err := core.FromString(s)
		if err != nil {
			return
		}
		got := core.String(u)
		u2, err := core.FromString(got)
		if err != nil {
			t.Fatalf("re-parse of canonical string failed: %v", err)
		}
		if core.Cmp(u, u2) != 0 {
			t.Fatalf("round-trip mismatch: %s => %s", s, got)
		}
	})
}

func TestAccountSize(t *testing.T) {
	if got := unsafe.Sizeof(core.Account{}); got != core.AccountStructSize {
		t.Fatalf("unexpected account size: got=%d want=%d", got, core.AccountStructSize)
	}
}
