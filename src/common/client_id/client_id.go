package clientid

import "strconv"

type ID struct {
	Server string `json:"server"`
	Seq    uint32 `json:"seq"`
}

func (id ID) String() string {
	return id.Server + "-" + strconv.FormatUint(uint64(id.Seq), 10)
}
