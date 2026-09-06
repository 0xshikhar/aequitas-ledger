# Phase 0 — Core Foundations & Primitives Deep-Dive

## 1. Objective & Problem Statement

Financial ledger systems require absolute, lossless precision. Double-entry accounting requires:
$$\sum \text{Debits} = \sum \text{Credits}$$
Standard floating-point numbers (`float64`) are **strictly prohibited** in financial software due to IEEE 754 binary rounding inaccuracies (e.g. `0.1 + 0.2 != 0.3`). Furthermore, 64-bit unsigned integers (`uint64`) max out at $18,446,744,073,709,551,615$ ($\approx 1.84 \times 10^{19}$). In micro-unit systems (such as financial ledger balances denominated in cents or crypto units), 64-bit integers risk overflow under heavy transaction volumes.

Phase 0 establishes the fundamental building blocks of **Aequitas Ledger**:
1. An arbitrary 128-bit unsigned integer arithmetic type (`Uint128`).
2. Fixed-size 64-byte `Account` data structure optimized for CPU L1 cache line alignment.
3. Immutable `Transfer` value representation.
4. CRC32-validated binary record codec for write-ahead logging.

---

## 2. Core Go Language Concepts & Engineering Internals

### A. 128-bit Arithmetic via Hardware CPU Carry/Borrow Primitives
Go does not natively feature a built-in `uint128` primitive. We construct `Uint128` using two 64-bit words:

```go
type Uint128 struct {
    Lo uint64
    Hi uint64
}
```

#### Addition with Hardware Carry (`bits.Add64`)
When adding two 128-bit numbers $(a_{hi}, a_{lo})$ and $(b_{hi}, b_{lo})$:
```go
func Add(a, b Uint128) (Uint128, error) {
    lo, carry := bits.Add64(a.Lo, b.Lo, 0)
    hi, carryHi := bits.Add64(a.Hi, b.Hi, carry)
    if carryHi != 0 {
        return Uint128{}, ErrUint128Overflow
    }
    return Uint128{Lo: lo, Hi: hi}, nil
}
```
* **Go Concept (`math/bits`)**: `bits.Add64` compiles directly to single CPU instructions on modern x86-64 (`ADC` - Add with Carry) and ARM64 (`ADCS`). This avoids branch mispredictions.

#### Subtraction with Hardware Borrow (`bits.Sub64`)
```go
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
```

---

### B. Memory Layout Alignment & Cache Line Padding
Modern CPU architectures fetch memory in **64-byte cache lines**. If a struct spans across two cache lines, reading it causes false sharing or dual cache line fetches.

```go
type Account struct {
	ID            [16]byte // 16 bytes
	Currency      [4]byte  // 4 bytes
	PostedDebits  Uint128  // 16 bytes
	PostedCredits Uint128  // 16 bytes
	Flags         uint32   // 4 bytes
	_             [4]byte  // 4 bytes explicit padding
}
```

#### Compile-Time Struct Size Guard
To guarantee `Account` never quietly grows or misaligns due to future field additions:
```go
var _ [64 - unsafe.Sizeof(Account{})]byte
```
* **Go Concept**: If `unsafe.Sizeof(Account{})` is anything other than `64`, the array length becomes negative or zero in invalid ways, breaking compilation instantly.

---

### C. Binary Serialization & Codec CRC32 Validation
JSON or XML parsing allocates dynamic memory on every record and requires expensive string conversions. `aequitas-ledger` uses a fixed binary frame codec:

$$\text{[Type (1B)]} \;\Vert\; \text{[Len (4B)]} \;\Vert\; \text{[Payload (NB)]} \;\Vert\; \text{[CRC32 (4B)]}$$

```go
func EncodeRecord(r Record) ([]byte, error) {
    frameLen := RecordHeaderSize + len(r.Payload) + RecordCRCSize
    out := make([]byte, frameLen)
    out[0] = byte(r.Type)
    binary.BigEndian.PutUint32(out[1:5], uint32(len(r.Payload)))
    copy(out[5:5+len(r.Payload)], r.Payload)

    checksum := crc32.ChecksumIEEE(out[:5+len(r.Payload)])
    binary.BigEndian.PutUint32(out[5+len(r.Payload):], checksum)
    return out, nil
}
```

---

## 3. Alternative Choices & Architectural Trade-offs

| Design Choice | Chosen Approach | Alternative Evaluated | Trade-off / Justification |
|---|---|---|---|
| **Numeric Precision** | Custom `Uint128` (fixed 16B) | `math/big.Int` or `float64` | `big.Int` forces heap allocations for every operation; `float64` suffers floating point rounding bugs. `Uint128` is stack-allocated with zero GC pressure. |
| **Serialization** | Binary BigEndian Codec | JSON / Protobuf / Gob | Binary frame parsing achieves zero allocations per record read during WAL replay. |
| **Data Alignment** | Fixed 64-byte `Account` | Variable-length struct | Exactly fits 1 CPU L1 cache line, preventing multi-line fetches during high-speed sequential iterations. |
