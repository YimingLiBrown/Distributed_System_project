package puddlestore

import (
	"bytes"
	"encoding/gob"
)

const (
	DIR uint64 = iota
	FILE
)

type GUID = string

const BLOCKSIZE = uint64(5)

type Inode struct {
	AGUID         string
	Name          string
	Type          uint64
	IndirectBlock GUID
	Size          uint64
}

type IndirectBlock struct {
	List []GUID
}
type DataBlock struct {
	Data []byte
}

func EncodeInode(inode Inode) ([]byte, error) {
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err := e.Encode(inode)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func EncodeIndirectBlock(ib IndirectBlock) ([]byte, error) {
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err := e.Encode(ib)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func EncodeDataBlock(db DataBlock) ([]byte, error) {
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err := e.Encode(db)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func DecodeInode(b []byte) (Inode, error) {
	buff := bytes.NewBuffer(b)
	var inode Inode
	dataDecoder := gob.NewDecoder(buff)
	err := dataDecoder.Decode(&inode)
	if err != nil {
		return Inode{}, err
	}
	return inode, nil
}

func DecodeIndirectBlock(b []byte) (IndirectBlock, error) {
	buff := bytes.NewBuffer(b)
	var ib IndirectBlock
	dataDecoder := gob.NewDecoder(buff)
	err := dataDecoder.Decode(&ib)
	if err != nil {
		return IndirectBlock{}, err
	}
	return ib, nil
}

func DecodeDataBlock(b []byte) (DataBlock, error) {
	buff := bytes.NewBuffer(b)
	var db DataBlock
	dataDecoder := gob.NewDecoder(buff)
	err := dataDecoder.Decode(&db)
	if err != nil {
		return DataBlock{}, err
	}
	return db, nil
}

func CreateDataBlock(data []byte) (res []DataBlock) {
	dataSize := uint64(len(data))
	i := uint64(0)
	for dataSize > BLOCKSIZE {
		tmp := DataBlock{
			Data: data[i*BLOCKSIZE : (i+1)*BLOCKSIZE],
		}
		i++
		res = append(res, tmp)
		dataSize -= BLOCKSIZE
	}
	sth := DataBlock{
		Data: data[i*BLOCKSIZE:],
	}
	res = append(res, sth)
	return res
}

func CreateIndirectBlock(datablocks []GUID) IndirectBlock {
	return IndirectBlock{
		List: datablocks,
	}
}

func CreateInode(aguid string, path string, typ uint64, size uint64, ib GUID) Inode {
	return Inode{
		AGUID:         aguid,
		Name:          path,
		Type:          typ,
		Size:          size,
		IndirectBlock: ib,
	}
}
