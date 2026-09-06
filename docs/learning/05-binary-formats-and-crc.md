# 05 — Binary Formats, Framing & CRC

**Files in focus:** `internal/wal/record.go`, `internal/core/codec.go`,
`internal/snapshot/snapshot.go`

A database's file format outlives every line of code that reads it. This
document covers how the project builds byte-level formats: why big-endian,
why length-prefix framing, what a CRC does and does not protect, and why
decoders must be strict.

---

## 1. The record frame

```go
// internal/wal/record.go
const (
    RecordHeaderSize = 13 // 8 bytes LSN + 1 byte type + 4 bytes payload length
    RecordCRCSize    = 4
)

func EncodeRecord(r Record) ([]byte, error) {
    frameLen := RecordHeaderSize + len(r.Payload) + RecordCRCSize
    out := make([]byte, frameLen)
    binary.BigEndian.PutUint64(out[0:8], r.LSN)
    out[8] = byte(r.Type)
    binary.BigEndian.PutUint32(out[9:13], uint32(len(r.Payload)))
    copy(out[13:13+len(r.Payload)], r.Payload)
    checksum := crc32.ChecksumIEEE(out[:13+len(r.Payload)])
    binary.BigEndian.PutUint32(out[13+len(r.Payload):], checksum)
    return out, nil
}
```

Design decisions, one per line:

- **Big-endian network byte order.** Fixed for all time; little-endian would
  also work but only if written down and kept forever. The important thing is
  that the format specifies it — `binary.BigEndian.Uint64` on decode must
  mirror `PutUint64` on encode.
- **Length-prefix framing.** The 4-byte payload length lets a reader walk
  back-to-back records in a segment without delimiters: read 13 header
  bytes, learn the payload size, skip ahead. Delimiters (newlines, magic
  separators) require scanning and can false-positive inside binary payload
  bytes.
- **CRC over header+payload, stored after.** The checksum covers everything
  that matters; the trailer placement lets a recovery scan compute the CRC
  range without knowing the type.
- **Fixed header with an LSN.** Each record carries its own LSN so recovery
  can order, skip, and watermark without trusting positional arithmetic —
  the original format lacked this and the LSN accounting drifted between the
  live path (per batch) and recovery (per record) until it was made explicit
  per record.

`DecodeRecord` mirrors the encoder and is *strict*:

```go
payloadLen := int(binary.BigEndian.Uint32(frame[9:13]))
expectedLen := RecordHeaderSize + payloadLen + RecordCRCSize
if payloadLen < 0 || len(frame) != expectedLen {
    return Record{}, ErrCorrupted
}
```

It rejects anything that is not exactly a frame — wrong length, zero type,
CRC mismatch. **Decoders are the trust boundary; encoders can be lenient,
decoders never.** (The same principle as C0.12's ID parsing: accept
exactly what you specify.)

## 2. Structs never touch the wire directly

Why not `binary.Write(f, binary.BigEndian, &transfer)`? Because Go struct
layout includes **padding** (`PostedDebits Uint128` after `Currency [4]byte`
forces 4 bytes of padding), and layout is a compiler detail that can change
with field order or architecture. Instead the codec walks offsets explicitly:

```go
// internal/core/codec.go
func EncodeTransferPayload(t Transfer) []byte {
    b := make([]byte, TransferPayloadSize)   // 108
    off := 0
    copy(b[off:off+16], t.ID[:]);            off += 16
    copy(b[off:off+16], t.DebitAccountID[:]);off += 16
    ...
    binary.BigEndian.PutUint64(b[off:off+8], uint64(t.Timestamp))
    ...
}
```

Verbose, yes — and that is the point. The format is explicit, `TransferPayloadSize`
is a constant that decoders assert against, and no compiler upgrade can
silently change the disk. The decode direction returns `ErrInvalidTransferPayload`
on any length mismatch. Round-trip tests (`tests/unit/record_test.go`) lock
the format; a fuzz target (`FuzzDecodeRecord_NoPanicOnRandomBytes`) feeds
random bytes and asserts the decoder *errors or succeeds — never panics*.

## 3. What CRC32 does and does not buy you

`crc32.ChecksumIEEE` detects: single flipped bits, most burst errors, and
*accidental* corruption from torn writes or bad cables. It does not defend
against: an adversary (not cryptographic — use HMAC/SHA-256 when the threat
model includes malice, e.g. the replication stream between nodes), correlated
craft (two-bit errors CRC can miss), or *semantic* corruption — a record that
is bit-perfect but wrong (a valid transfer of the wrong amount). CRC says
"the bytes arrived as written"; only the state machine and invariants say
"the bytes were right."

The replication path (C0.8) applies the identical frame+CRC over TCP: TCP
checksums are weak and end-to-end verification is cheap insurance.

## 4. Versioning from day one

```go
// internal/snapshot/snapshot.go
const MagicHeader = "LEDGER01"
```

Every durable format gets a magic/version field *now*, while there is
exactly one version. Retrofitting a version field into a format that already
has petabytes on disk is a migration project; adding `LEDGER02` later is a
new branch in the reader. The decoder checks the magic and fails loudly on
mismatch — fail-fast on unknown versions beats guessing (P5.7 in the roadmap
extends this to a written compatibility policy).

## 5. Hand-rolled framing in the replication stream — and its lesson

`internal/replication/server.go` builds frames manually:

```go
buf := make([]byte, frameLen)
binary.BigEndian.PutUint64(buf[0:8], r.LSN)
buf[8] = byte(r.Type)
binary.BigEndian.PutUint32(buf[9:13], uint32(payloadLen))
copy(buf[13:13+payloadLen], r.Payload)
```

and the follower reads with `io.ReadFull` twice (13-byte header, then
payload+CRC). Note the duplication: three codecs now describe
transfer/account layouts (`core/codec.go`, `engine/codec.go` delegating to
it, and a dead `decodeTransferPayload` in the replication package —
C0.10's cleanup list). **Every codec duplication is a drift bug in waiting**:
the dead replication decoder, for instance, happily read payloads the real
codec would reject. When two implementations must agree, keep one and import
it.

> **Exercise 1.** Add a `RecordTypeFoo` and a version byte to the WAL record
> header. What breaks? (Hint: `RecordHeaderSize` changes, every existing
> segment is unreadable, and `peekSegmentMaxLSN`'s frame walk must track it.
> Now you understand why the version byte should have been there first.)
>
> **Exercise 2.** Write `FuzzDecodeTransferPayload` mirroring
> `FuzzDecodeRecord_NoPanicOnRandomBytes`. Run it for 30 seconds. Corrupt a
> payload mid-transfer field — does the CRC catch it? Does the decoder?

## Recap

| Concept | Where | Rule of thumb |
|---|---|---|
| Explicit endianness | `binary.BigEndian` everywhere | Specify once, mirror forever |
| Length-prefix framing | 13-byte record header | Frame by length, not by delimiter |
| Strict decoding | `DecodeRecord`, `parseHexID` | Encoders may be flexible; decoders reject everything else |
| Offset-walking codecs | `core/codec.go` | Never serialize Go structs directly (padding) |
| CRC32 scope | WAL + replication | Detects accidents, not adversaries; not semantic correctness |
| Format versioning | `LEDGER01` magic | Version field exists before version 2 |
| One codec, one owner | `core/codec.go` | Duplicated encoders drift; import, don't copy |
