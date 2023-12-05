package src

import (
	"math"
	"runtime"
	"strconv"
	"sync"
)

type SimulateEngine struct {
	acgs   []ACG
	buffer map[string]int
	blocks []*Block
}

func newSimulateEngine(blocks []*Block) *SimulateEngine {
	e := new(SimulateEngine)
	e.acgs = make([]ACG, 0)

	e.buffer = make(map[string]int, 0)
	e.blocks = blocks
	return e
}

// SimulateExecution
func (e *SimulateEngine) SimulateExecution() []ACG {
	parallelingNumber := int(math.Min(float64(runtime.NumCPU()), float64(config.BlockSize)))
	for _, block := range e.blocks {
		for j := 0; j < len(block.txs); j += parallelingNumber {
			var wg4tx sync.WaitGroup
			wg4tx.Add(parallelingNumber)
			for k := 0; k < parallelingNumber; k++ {
				go func(index int, bias int, wg4tx *sync.WaitGroup, buffer map[string]int) {
					defer wg4tx.Done()
					if index+bias >= len(block.txs) {
						return
					}
					tx := block.txs[index+bias]
					for _, op := range tx.Ops {
						if op.Type == OpRead {
							readResult, exist := buffer[op.Key]
							if !exist {
								readResult, _ = strconv.Atoi(globalSmallBank.Read(op.Key))
							}
							op.ReadResult = strconv.Itoa(readResult)
						}
						if op.Type == OpWrite {
							readResult, exist := buffer[op.Key]
							if !exist {
								readResult, _ = strconv.Atoi(globalSmallBank.Read(op.Key))
							}
							amount, _ := strconv.Atoi(op.Val)
							WriteResult := readResult + amount
							//buffer[op.Key] = WriteResult
							op.WriteResult = strconv.Itoa(WriteResult)
							//globalSmallBank.Write(op.Key, strconv.Itoa(WriteResult))
						}
					}
				}(j, k, &wg4tx, e.buffer)
			}
			wg4tx.Wait()
		}
		nezha := newNeZha(block.txs)
		nezha.TransactionSort()
		for address, stateSet := range nezha.acg {
			writeSet := stateSet.WriteSet
			if len(writeSet) == 0 {
				continue
			}
			flag := false
			for i := len(writeSet) - 1; i >= 0; i-- {
				if !writeSet[i].tx.abort {
					e.buffer[address], _ = strconv.Atoi(writeSet[i].op.WriteResult)
					flag = true
					break
				}
			}
			if !flag {
				writeSet = make([]Unit, 0)
				stateSet.WriteSet = writeSet
				delete(e.buffer, address)
			}
		}
		e.acgs = append(e.acgs, nezha.acg)
	}
	return e.acgs
}
