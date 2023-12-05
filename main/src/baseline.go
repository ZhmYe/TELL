package src

import (
	"time"
)

// baseline
// Start processing blocks until all instances agree to complete at least one block
func (peer *Peer) runInBaseline() {
	// start all instances
	for i := 0; i < len(peer.instances); i++ {
		peer.instances[i].start()
	}
	peer.UpdateLastExecutionTime()
	index := 0
	for {
		if peer.checkFinished() {
			break
		}
		if peer.baselineCheck(index+1) && peer.checkExecutionTimeout() {
			peer.UpdateLastExecutionTime()
			// start a new epoch
			//for _, instance := range peer.instances {
			//	fmt.Print(len(instance.blocks) - instance.hasExecutedIndex)
			//	fmt.Print(" ")
			//}
			//fmt.Println()
			//startTime := time.Now()
			txs := make([]*Transaction, 0)
			for _, instance := range peer.instances {
				txs = append(txs, instance.blocks[index].txs...)
			}
			blocks := make([]*Block, 0)
			blocks = append(blocks, newBlock(txs))
			simulateEngine := newSimulateEngine(blocks)
			simulateEngine.SimulateExecution()
			abortTxs := make([]*Transaction, 0)
			//writeAddress := make(map[string]bool, 0)
			for i, _ := range peer.instances {
				//peer.instances[index].CascadeAbort(&writeAddress)
				tmp := peer.instances[i].execute(1)
				abortTxs = append(abortTxs, tmp...)
			}
			peer.reExecute(abortTxs)
			//finishTime := time.Now()
			for _, instance := range peer.instances {
				instance.blocks[index].finishTime = time.Since(instance.blocks[index].createTime)
				instance.hasExecutedIndex++
			}
			index++
		}

	}
}
func (peer *Peer) baselineCheck(index int) bool {
	total := 0
	for _, instance := range peer.instances {
		if len(instance.blocks) >= index {
			total += 1
		}
	}
	return total == peer.instanceNumber
}
