package integration

import "encoding/binary"

func mkID(v uint64) [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[8:], v)
	return id
}
