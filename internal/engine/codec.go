package engine

import (
	"aequitas-ledger/internal/core"
)

func EncodeTransferPayload(t core.Transfer) []byte {
	return core.EncodeTransferPayload(t)
}

func DecodeTransferPayload(b []byte) (core.Transfer, error) {
	return core.DecodeTransferPayload(b)
}

func EncodeAccountPayload(a core.Account) []byte {
	return core.EncodeAccountPayload(a)
}

func DecodeAccountPayload(b []byte) (core.Account, error) {
	return core.DecodeAccountPayload(b)
}
