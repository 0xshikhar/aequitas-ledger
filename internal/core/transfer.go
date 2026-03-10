package core

type Transfer struct {
	ID              [16]byte
	DebitAccountID  [16]byte
	CreditAccountID [16]byte
	Amount          Uint128
	IdempotencyKey  [32]byte
	Timestamp       int64
	Flags           uint32
	_               [4]byte
}
