package src

import "time"

// OpType 操作类型
type OpType int

const (
	OpRead OpType = iota
	OpWrite
)

// Op 操作
type Op struct {
	Type        OpType
	Key         string
	Val         string
	ReadResult  string
	WriteResult string
}

type TxType int

const (
	transactSavings TxType = iota
	depositChecking
	sendPayment
	writeCheck
	amalgamate
	query
)

// Transaction
type Transaction struct {
	txType   TxType
	Ops      []*Op
	abort    bool
	sequence int
	txHash   string
	id       int
}

// Block 区块
type Block struct {
	txs        []*Transaction
	createTime time.Time
	finish     bool
	finishTime time.Duration
}

func newBlock(txs []*Transaction) *Block {
	block := new(Block)
	block.txs = txs
	block.createTime = time.Now()
	block.finish = false
	return block
}
