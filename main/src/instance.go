package src

import (
	"strconv"
	"sync"
	"time"
)

// Instance
type Instance struct {
	blocks                []*Block                     // blocks
	timeout               time.Duration                // Block out time interval, ms
	hasExecutedIndex      int                          // The latest executed block index defaults to 0
	lastBlockOutTimeStamp time.Time                    // Last block output time
	id                    int                          // instance id
	maxBlockNumber        int                          // Maximum number of blocks
	simulate              *SimulateEngine              // SimulateEngine instance
	acgs                  []ACG                        // Simulate the ACG of all sub blocks executed
	record                map[string]map[string][]Unit // Count the number of read operations directly connected after each address for each transaction
	cascade               map[string]int               // Cascade degree at each address
	finish                bool
}

func newInstance(timeout time.Duration, id int) *Instance {
	instance := new(Instance)
	instance.blocks = make([]*Block, 0)
	instance.lastBlockOutTimeStamp = time.Now()
	instance.timeout = timeout * time.Millisecond
	instance.hasExecutedIndex = 0
	instance.id = id
	instance.maxBlockNumber = 24
	instance.finish = false
	return instance
}

func (instance *Instance) checkTimeout() bool {
	if time.Since(instance.lastBlockOutTimeStamp) >= instance.timeout {
		return true
	}
	return false
}

func (instance *Instance) updateLastBlockOutTimeStamp() {
	instance.lastBlockOutTimeStamp = time.Now()
}

func (instance *Instance) blockOut() {
	if len(instance.blocks) >= instance.maxBlockNumber {
		return
	}
	txs := globalSmallBank.GenTxSet(config.BlockSize)
	block := newBlock(txs)
	instance.blocks = append(instance.blocks, block)
}
func (instance *Instance) start() {
	// here, in some Evaluation, should delete "go func()" to simply test.
	//go func() {
	instance.updateLastBlockOutTimeStamp()
	for {
		if len(instance.blocks) >= instance.maxBlockNumber {
			instance.finish = true
			//fmt.Println("Instance " + strconv.Itoa(instance.id) + " finished...")
			break
		}
		if instance.checkTimeout() {
			instance.blockOut()
			instance.updateLastBlockOutTimeStamp()
		}
	}
	//}()
}
func (instance *Instance) simulateExecution(number int) int {
	lastIndex := instance.hasExecutedIndex + number
	if instance.hasExecutedIndex == len(instance.blocks) {
		return 0
	}
	if instance.hasExecutedIndex+number > len(instance.blocks) {
		lastIndex = len(instance.blocks)
	}
	instance.simulate = newSimulateEngine(instance.blocks[instance.hasExecutedIndex:lastIndex])
	instance.acgs = instance.simulate.SimulateExecution()
	//instance.record, instance.cascade = computeCascade(instance.acgs)
	return lastIndex - instance.hasExecutedIndex
}
func (instance *Instance) abortReadSet(readSet []Unit) {
	repeatCheck := make(map[string]bool)
	if len(readSet) == 0 {
		return
	}
	for _, unit := range readSet {
		_, repeat := repeatCheck[unit.tx.txHash]
		if repeat || unit.tx.abort {
			continue
		}
		repeatCheck[unit.tx.txHash] = true
		unit.tx.abort = true
		CascadeInAddress, haveCascade := instance.record[unit.tx.txHash]
		if haveCascade {
			for _, eachReadSet := range CascadeInAddress {
				instance.abortReadSet(eachReadSet)
			}
		}

	}
}
func (instance *Instance) CascadeAbort(writeAddress *map[string]bool) {
	hasAbort := make(map[string]bool, 0)
	localWriteAddress := make([]string, 0) // The write set involved in the current ACGS, used to update the writeAddress
	for _, acg := range instance.acgs {
		for address, stateSet := range acg {
			if len(stateSet.WriteSet) != 0 {
				localWriteAddress = append(localWriteAddress, address)
			}
			_, exist := (*writeAddress)[address]
			// If the address written by the top ranked instance is read, and it is the first read ACG
			if exist {
				if len(stateSet.ReadSet) != 0 {
					_, has := hasAbort[address]
					if !has {
						hasAbort[address] = true
						instance.abortReadSet(stateSet.ReadSet)
					}
				}
			}
		}
	}
	for _, address := range localWriteAddress {
		(*writeAddress)[address] = true
	}
}
func (instance *Instance) getAbortTxs(n int) []*Transaction {
	abortTxs := make([]*Transaction, 0)
	for _, block := range instance.blocks[instance.hasExecutedIndex : instance.hasExecutedIndex+n] {
		for _, tx := range block.txs {
			if tx.abort {
				abortTxs = append(abortTxs, tx)
			}
		}
	}
	return abortTxs
}
func (instance *Instance) execute(n int) []*Transaction {
	//for _, acg := range instance.acgs {
	//	for address, stateSet := range acg {
	//		if len(stateSet.WriteSet) != 0 {
	//			globalSmallBank.Write(address, stateSet.WriteSet[len(stateSet.WriteSet)-1].op.WriteResult)
	//		}
	//	}
	//}
	abortTxs := make([]*Transaction, 0)
	lastIndex := instance.hasExecutedIndex + n
	if instance.hasExecutedIndex == len(instance.blocks) {
		return abortTxs
	}
	if instance.hasExecutedIndex+n > len(instance.blocks) {
		lastIndex = len(instance.blocks)
	}
	for _, block := range instance.blocks[instance.hasExecutedIndex : instance.hasExecutedIndex+n] {
		block.finishTime = time.Since(block.createTime)
		tmp := len(block.txs) - len(block.txs)%config.parallelingNumber + config.parallelingNumber
		for j := 0; j < tmp; j += config.parallelingNumber {
			var wg4tx sync.WaitGroup
			wg4tx.Add(config.parallelingNumber)
			for k := 0; k < config.parallelingNumber; k++ {
				if j+k >= len(block.txs) {
					wg4tx.Done()
					continue
				}
				tmpTx := block.txs[j+k]
				go func(tx *Transaction) {
					defer wg4tx.Done()
					for _, op := range tx.Ops {
						if op.Type == OpWrite {
							globalSmallBank.Write(op.Key, op.WriteResult)
						}
					}
				}(tmpTx)
			}
			wg4tx.Wait()
		}
		for _, tx := range block.txs {
			if tx.abort {
				abortTxs = append(abortTxs, tx)
			}
		}
		//for _, tx := range block.txs {
		//	if tx.abort {
		//		abortTxs = append(abortTxs, tx)
		//		continue
		//	}
		//	for _, op := range tx.Ops {
		//		if op.Type == OpWrite {
		//			globalSmallBank.Write(op.Key, op.WriteResult)
		//		}
		//	}
		//}
		block.finish = true
	}
	instance.hasExecutedIndex = lastIndex
	return abortTxs
}
func (instance *Instance) reExecute(txs []*Transaction) {
	for _, tx := range txs {
		for _, op := range tx.Ops {
			if op.Type == OpRead {
				op.ReadResult = globalSmallBank.Read(op.Key)
			} else {
				readResult, _ := strconv.Atoi(globalSmallBank.Read(op.Key))
				amount, _ := strconv.Atoi(op.Val)
				op.WriteResult = strconv.Itoa(readResult + amount)
				globalSmallBank.Write(op.Key, op.WriteResult)
			}
		}
	}
}
