package src

import (
	"strconv"
	"sync"
	"time"
)

//var globalExecutionSync = false

type Peer struct {
	instances         []Instance    // 一个节点维护n个共识instance
	instanceNumber    int           // instance数量
	timeDuration      time.Duration // 执行timeout
	lastExecutionTime time.Time     // 上一次运行执行的时间
}

func newPeer(n int) *Peer {
	peer := new(Peer)
	peer.instanceNumber = n
	peer.instances = make([]Instance, 0)
	peer.timeDuration = time.Duration(40) * time.Millisecond
	peer.UpdateLastExecutionTime()
	for i := 0; i < n; i++ {
		instance := newInstance(time.Duration(int(float64(40)/float64(i+1))), i)
		peer.instances = append(peer.instances, *instance)
	}
	return peer
}
func (peer *Peer) UpdateLastExecutionTime() {
	peer.lastExecutionTime = time.Now()
}
func (peer *Peer) checkExecutionTimeout() bool {
	if time.Since(peer.lastExecutionTime) >= peer.timeDuration {
		return true
	}
	return false
}
func (peer *Peer) checkFinished() bool {
	total := 0
	for _, instance := range peer.instances {
		if instance.finish && len(instance.blocks) == instance.hasExecutedIndex {
			total += 1
		}
	}
	if total == peer.instanceNumber {
		return true
	}
	return false
}
func (peer *Peer) run() (time.Duration, time.Duration, time.Duration) {
	// start all instances
	for i := 0; i < len(peer.instances); i++ {
		peer.instances[i].start()
	}
	peer.UpdateLastExecutionTime()
	for {
		if peer.checkFinished() {
			break
		}
		if peer.checkExecutionTimeout() {
			peer.UpdateLastExecutionTime()
			// start a new epoch
			//for _, instance := range peer.instances {
			//	fmt.Print(len(instance.blocks) - instance.hasExecutedIndex)
			//	fmt.Print(" ")
			//}
			//fmt.Println()
			//startTime := time.Now()
			var wg4Execution sync.WaitGroup
			wg4Execution.Add(len(peer.instances))
			execBlockNumber := make([]int, peer.instanceNumber)
			for i := 0; i < len(peer.instances); i++ {
				go func(index int, wg *sync.WaitGroup) {
					defer wg.Done()
					n := peer.instances[index].simulateExecution(index + 1)
					execBlockNumber[index] = n
				}(i, &wg4Execution)
			}
			wg4Execution.Wait()
			//fmt.Print("Simulate Time:")
			//fmt.Print(time.Since(startTime))
			//fmt.Print(" ")
			startTime := time.Now()
			cascade := make(map[string][]int)
			for _, address := range globalSmallBank.savings {
				//cascade[address] = make([]int, peer.instanceNumber)
				tmp := make([]int, peer.instanceNumber)
				flag := false
				for i := 0; i < len(peer.instances); i++ {
					localCascade, exist := peer.instances[i].cascade[address]
					if exist {
						if localCascade != 0 {
							flag = true
						}
						//flag = true
						tmp[i] = localCascade
					} else {
						tmp[i] = 0
					}
				}
				if flag {
					cascade[address] = tmp
				}
			}
			for _, address := range globalSmallBank.checkings {
				//cascade[address] = make([]int, peer.instanceNumber)
				tmp := make([]int, peer.instanceNumber)
				flag := false
				for i := 0; i < len(peer.instances); i++ {
					localCascade, exist := peer.instances[i].cascade[address]
					if exist {
						if localCascade != 0 {
							flag = true
						}
						//flag = true
						tmp[i] = localCascade
					} else {
						tmp[i] = 0
					}
				}
				if flag {
					cascade[address] = tmp
				}
			}
			OrderGraph := make([][]int, peer.instanceNumber)
			for i := 0; i < peer.instanceNumber; i++ {
				OrderGraph[i] = make([]int, peer.instanceNumber)
				for j := 0; j < peer.instanceNumber; j++ {
					OrderGraph[i][j] = 0
				}
			}
			for _, cascades := range cascade {
				// sort cascades
				index := make([]int, peer.instanceNumber)
				for i := 0; i < peer.instanceNumber; i++ {
					index[i] = i
				}
				for i := 0; i < peer.instanceNumber; i++ {
					for j := i + 1; j < peer.instanceNumber; j++ {
						if cascades[i] < cascades[j] {
							index[i], index[j] = index[j], index[i]
						}
					}
				}
				//
				for i := 0; i < peer.instanceNumber-1; i++ {
					pre := index[i]
					latter := index[i+1]
					// if not sorted before
					if OrderGraph[latter][pre] == 0 {
						OrderGraph[pre][latter] = 1
						// no cycle
						for j := 0; j < peer.instanceNumber; j++ {
							if OrderGraph[j][pre] == 1 {
								OrderGraph[j][latter] = 1
							}
						}
					}
				}
			}
			//fmt.Println(OrderGraph)
			order := TopologicalOrder(OrderGraph)
			//fmt.Println(order)
			abortTxs := make([]*Transaction, 0)
			writeAddress := make(map[string]bool, 0)
			//fmt.Print("Combine Time:")
			//fmt.Print(time.Since(startTime))
			//fmt.Print(" ")
			combineTime := time.Since(startTime)
			startTime = time.Now()
			for _, index := range order {
				peer.instances[index].CascadeAbort(&writeAddress)
				txs := peer.instances[index].execute(execBlockNumber[index])
				abortTxs = append(abortTxs, txs...)
			}
			//fmt.Print("Execute Time:")
			//fmt.Print(time.Since(startTime))
			//fmt.Print(" ")
			commitTime := time.Since(startTime)
			startTime = time.Now()
			peer.reExecute(abortTxs)
			//fmt.Print("ReExecute Time:")
			//fmt.Print(time.Since(startTime))
			//fmt.Print(" ")
			////startTime = time.Now()
			//fmt.Println()
			reExecuteTime := time.Since(startTime)
			//fmt.Println(time.Since(startTime))
			return combineTime, commitTime, reExecuteTime
		}
	}
	return time.Duration(0), time.Duration(0), time.Duration(0)
}

func (peer *Peer) reExecute(txs []*Transaction) {
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
