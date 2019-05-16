package hashmachine

import (
	"errors"

	"github.com/google/uuid"
)

const (
	HASH_CHAIN_CREATE uint64 = iota
	HASH_CHAIN_READ
	HASH_CHAIN_DELETE
)

type HashMachine struct {
	guids map[string]string
}

func (h *HashMachine) create(path []byte) (vguid string, err error) {
	if len(h.guids) == 0 {
		h.guids = make(map[string]string)
	}

	aguid := string(path[:])
	vguid = uuid.New().String()
	h.guids[aguid] = vguid
	return vguid, nil
}

func (h *HashMachine) read(path []byte) (vguid string, err error) {
	aguid := string(path[:])
	vguid = h.guids[aguid]
	if len(vguid) == 0 {
		return vguid, errors.New("No data")
	}
	return vguid, nil
}

func (h *HashMachine) delete(path []byte) (vguid string, err error) {
	aguid := string(path[:])

	if _, ok := h.guids[aguid]; ok {
		delete(h.guids, aguid)
		return "", nil
	} else {
		return "", errors.New("No such key")
	}
}

func (h *HashMachine) GetState() (state interface{}) {
	return h.guids
}
func (h *HashMachine) ApplyCommand(command uint64, path []byte) (message string, err error) {
	switch command {
	case HASH_CHAIN_CREATE:
		return h.create(path)
	case HASH_CHAIN_READ:
		return h.read(path)
	case HASH_CHAIN_DELETE:
		return h.delete(path)
	default:
		return "", errors.New("unknown command type")
	}
}

func (h *HashMachine) FormatCommand(command uint64) (commandString string) {
	switch command {
	case HASH_CHAIN_CREATE:
		return "HASH_CHAIN_CREATE"
	case HASH_CHAIN_READ:
		return "HASH_CHAIN_READ"
	case HASH_CHAIN_DELETE:
		return "HASH_CHAIN_DELETE"
	default:
		return "UNKNOWN_COMMAND"
	}
}
