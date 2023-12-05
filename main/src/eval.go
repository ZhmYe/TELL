package src

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

type algorithmType int

const (
	BasicFabric algorithmType = iota
	FabricPlusPlus
	Nezha
)

// AbortRate_RunningTime_Evaluation
// Experiment on testing the termination rate and execution time of fabric, fabric++, nezha, and Harmony
func AbortRate_RunningTime_Evaluation() ([4][6][5]time.Duration, [4][6][5]float64) {
	//smallbank := TestSmallbank(false)
	blockSize := 200
	timeResult := new([4][6][5]time.Duration)
	rateResult := new([4][6][5]float64)
	for block := 0; block < 6; block++ {
		txNumber := blockSize * (block*2 + 2)
		for hotRate := 0; hotRate < 5; hotRate++ {
			config.ZipfianConstant = float64(hotRate)*0.2 + 0.2
			if config.ZipfianConstant == 1 {
				config.ZipfianConstant = 0.999
			}
			globalSmallBank.UpdateZipfian()
			for a := 0; a <= 3; a++ {
				startTime := time.Now()
				rate := float64(0)
				for i := 0; i < 10; i++ {
					txs := globalSmallBank.GenTxSet(txNumber)
					switch a {
					case 0:
						f := newFabric(txs)
						f.TransactionSort()
						rate += f.getAbortRate()
					case 1:
						h := newHarmony(txs)
						h.TransactionSort()
						rate += h.getAbortRate()
					case 2:
						f := newFabricPP(txs)
						f.TransactionSort()
						rate += f.getAbortRate()
					case 3:
						f := newNeZha(txs)
						f.TransactionSort()
						rate += f.getAbortRate()
					}
				}
				timeResult[a][block][hotRate] = time.Since(startTime) / 10
				rateResult[a][block][hotRate] = rate / 10
			}
		}
	}
	fmt.Println(timeResult[0])
	fmt.Println(rateResult[0])
	return *timeResult, *rateResult
}

// Instance_Not_Miss_Evaluation
// Hit rate under different probing execution levels and conflict rates
// Simply for one Instance
func Instance_Not_Miss_Evaluation() {
	// Taking sketch=0.6, 0.8, 0.99, the instance generates up to 11 blocks. The first block is assumed to arrive slowly without execution, and the subsequent n-1 blocks are executed first
	// Hit rate test, first extract the read and write sets of all transactions in the first block, and then extract the read and write sets of the following n-1 blocks to obtain the intersection of the latter read set and the former write set
	// Hit rate= | Intersection | / | The latter reads the set|
	for skew := 0.6; skew <= 1; skew += 0.2 {
		fmt.Println("	skew=" + strconv.FormatFloat(skew, 'f', 2, 64))
		if skew == 1 {
			skew = 0.99
		}
		config.ZipfianConstant = skew
		globalSmallBank.UpdateZipfian()
		instance := newInstance(time.Duration(10), 0)
		//instance.maxBlockNumber = 11 // 试探性执行度1 ~ 10
		instance.start()

		firstBlock := instance.blocks[0]
		preWriteSet := make(map[string]bool, 0)
		for _, tx := range firstBlock.txs {
			for _, op := range tx.Ops {
				if op.Type == OpWrite {
					preWriteSet[op.Key] = true
				}
			}
		}
		for n := 1; n <= 10; n++ {
			notMiss := 0
			latterReadSet := make(map[string]bool, 0)
			for i := 1; i <= n; i++ {
				block := instance.blocks[i]
				for _, tx := range block.txs {
					for _, op := range tx.Ops {
						if op.Type == OpRead {
							latterReadSet[op.Key] = true
							_, exist := preWriteSet[op.Key]
							if exist {
								notMiss++
							}
						}
					}
				}
			}
			fmt.Println("		试探性执行度: " + strconv.Itoa(n) + " , 命中率: " + strconv.FormatFloat(float64(notMiss)/float64(len(latterReadSet))*100, 'f', 2, 64) + "%")
		}
	}
}

// Instance_Abort_Evaluation for a single instance
// Abortion rates under different levels of exploratory execution and conflict rates
// For the convenience of testing, we generate n blocks, with the first n-1 blocks executed first and the last block used as the first block in the experiment
// when run this evaluation, the code in Peer.go should be modified. (no endless loop and a certain number of blocks)
func Instance_Abort_Evaluation() {
	for skew := 0.6; skew <= 1; skew += 0.2 {
		fmt.Println("	skew=" + strconv.FormatFloat(skew, 'f', 2, 64))
		if skew == 1 {
			skew = 0.99
		}
		config.ZipfianConstant = skew
		globalSmallBank.UpdateZipfian()
		for n := 1; n <= 10; n++ {
			instance := newInstance(time.Duration(10), 0)
			instance.start()
			instance.simulateExecution(n)
			abort := 0
			firstBlock := instance.blocks[n]
			writeAddress := make(map[string]bool, 0)
			for _, tx := range firstBlock.txs {
				for _, op := range tx.Ops {
					if op.Type == OpWrite {
						writeAddress[op.Key] = true
					}
				}
			}
			instance.CascadeAbort(&writeAddress)
			for _, block := range instance.blocks[:n] {
				for _, tx := range block.txs {
					if tx.abort {
						abort++
					}
				}
			}
			//nezha := newNeZha(instance.blocks[n].txs)
			//nezha.TransactionSort()
			//for _, tx := range nezha.txs {
			//	if tx.abort {
			//		abort++
			//	}
			//}
			fmt.Print(abort)
			fmt.Print(" ")
			//fmt.Println()
			fmt.Println(float64(abort) / float64((n)*config.BlockSize))
		}
	}
}
func Instance_ReExectution_Time_Evaluation() {
	for skew := 0.6; skew <= 1; skew += 0.2 {
		fmt.Println("	skew=" + strconv.FormatFloat(skew, 'f', 2, 64))
		if skew == 1 {
			skew = 0.99
		}
		config.ZipfianConstant = skew
		globalSmallBank.UpdateZipfian()
		for n := 1; n <= 10; n++ {
			combine := time.Duration(0)
			reExecution := time.Duration(0)
			for i := 0; i < 100; i++ {
				instance := newInstance(time.Duration(10), 0)
				instance.start()
				instance.simulateExecution(n)
				s := newSimulateEngine(instance.blocks[n : n+1])
				s.SimulateExecution()
				instance.acgs = append(instance.acgs, s.acgs[0])
				firstBlock := instance.blocks[n]
				writeAddress := make(map[string]bool, 0)
				startTime := time.Now()
				for _, tx := range firstBlock.txs {
					if tx.abort {
						continue
					}
					for _, op := range tx.Ops {
						if op.Type == OpWrite {
							writeAddress[op.Key] = true
						}
					}
				}
				instance.CascadeAbort(&writeAddress)
				abortTxs := instance.getAbortTxs(n + 1)
				//instance.execute(n + 1)
				//fmt.Print("Combine Time")
				combine += time.Since(startTime)
				//fmt.Print(time.Since(startTime) / 100)
				//fmt.Println()
				startTime = time.Now()
				instance.reExecute(abortTxs)
				reExecution += time.Since(startTime)
				//fmt.Print(" ReExecute Time")
				//fmt.Println(time.Since(startTime) / 100)
				//fmt.Println()
			}
			//fmt.Print("Combine Time: ")
			//fmt.Print(combine / 100)
			//fmt.Print(" ReExecute Time: ")
			//fmt.Println(reExecution / 100)
		}
	}
}
func Instance_Execution_Time_Evaluation() {
	for skew := 0.6; skew <= 1; skew += 0.2 {
		fmt.Println("	skew=" + strconv.FormatFloat(skew, 'f', 2, 64))
		if skew == 1 {
			skew = 0.99
		}
		config.ZipfianConstant = skew
		globalSmallBank.UpdateZipfian()
		for n := 2; n <= 11; n++ {
			instance := newInstance(time.Duration(10), 0)
			instance.start()
			startTime := time.Now()
			for i := 0; i < 100; i++ {
				instance.simulateExecution(n)
				abortTxs := instance.execute(n)
				instance.reExecute(abortTxs)
			}
			fmt.Println(time.Since(startTime) / 100)
		}
	}
}

// Instance_Waiting_Time_Evalutation
// Test the waiting time of instances with different speeds and block heights for the new solution
func Instance_Waiting_Time_Evalutation() {
	peer := newPeer(4)
	peer.run()
	for _, instance := range peer.instances {
		for i, block := range instance.blocks {
			if i >= 20 {
				continue
			}
			fmt.Print("Block Height: ")
			fmt.Print(i)
			fmt.Print(" Waiting Time: ")
			fmt.Println(block.finishTime)
		}
	}
}

// Baseline_Waiting_Time_Evalutation
// Testing the waiting time of instances with different speeds and block heights in the baseline
func Baseline_Waiting_Time_Evalutation() {
	peer := newPeer(4)
	peer.runInBaseline()
	for _, instance := range peer.instances {
		for i, block := range instance.blocks {
			if i >= 24 {
				continue
			}
			fmt.Print("Block Height: ")
			fmt.Print(i)
			fmt.Print(" Waiting Time: ")
			fmt.Println(block.finishTime)
		}
	}
}

// Instance_Not_Execute_Block_Number_Evaluation
// Test the number of blocks that have not been executed over time for instances with different speeds in the new solution
func Instance_Not_Execute_Block_Number_Evaluation() {
	peer := newPeer(4)
	peer.run()
}

// Baseline_Not_Execute_Block_Number_Evaluation
// Test the number of blocks that have not been executed over time for instances with different speeds in the baseline
func Baseline_Not_Execute_Block_Number_Evaluation() {
	peer := newPeer(4)
	peer.runInBaseline()
}

// Instance_Number_Abort_Rate_Evaluation 测试不同instance数量(并发度)、冲突率下的abort rate
func Instance_Number_Abort_Rate_Evaluation() {
	// 每个instance只出一个块
	for skew := 0.6; skew <= 1; skew += 0.2 {
		fmt.Println("	skew=" + strconv.FormatFloat(skew, 'f', 2, 64))
		if skew == 1 {
			skew = 0.99
		}
		config.ZipfianConstant = skew
		globalSmallBank.UpdateZipfian()
		for concurrency := 1; concurrency <= 8; concurrency++ {
			peer := newPeer(concurrency)
			peer.run()
			// 每个instance只有一个块，取出后计算abort rate
			abort := 0
			for _, instance := range peer.instances {
				for _, block := range instance.blocks {
					for _, tx := range block.txs {
						if tx.abort {
							abort++
						}
					}
				}
			}
			fmt.Println(float64(abort) / float64(peer.instanceNumber*config.BlockSize))
		}
	}
}

// Instance_Number_Time_Evaluation
// Test the latency of each stage under different instances (concurrency) and conflicts
func Instance_Number_Time_Evaluation() {
	// 每个instance只出一个块
	for size := 1; size <= 3; size++ {
		config.BlockSize = int(100 * math.Pow(float64(2), float64(size)))
	}
	fmt.Println("block size=" + strconv.Itoa(config.BlockSize))
	for skew := 0.6; skew <= 1; skew += 0.2 {
		fmt.Println("skew=" + strconv.FormatFloat(skew, 'f', 2, 64))
		if skew == 1 {
			skew = 0.99
		}
		config.ZipfianConstant = skew
		globalSmallBank.UpdateZipfian()
		for concurrency := 2; concurrency <= 8; concurrency++ {
			combine, commit, reExecute := time.Duration(0), time.Duration(0), time.Duration(0)
			fmt.Print("concurrency=" + strconv.Itoa(concurrency) + " ")
			for i := 0; i < 100; i++ {
				peer := newPeer(concurrency)
				combineTime, commitTime, reExecuteTime := peer.run()
				combine += combineTime
				commit += commitTime
				reExecute += reExecuteTime
			}
			fmt.Println(combine/100, commit/100, reExecute/100)
		}
	}
	//}
}
func Instance_Number_tps_Evaluation() {
	// each instance get only one block
	for skew := 0.6; skew <= 1; skew += 0.2 {
		fmt.Println("	skew=" + strconv.FormatFloat(skew, 'f', 2, 64))
		if skew == 1 {
			skew = 0.99
		}
		config.ZipfianConstant = skew
		globalSmallBank.UpdateZipfian()
		for concurrency := 2; concurrency <= 8; concurrency++ {
			peer := newPeer(concurrency)
			startTime := time.Now()
			peer.run()
			duration := time.Since(startTime)
			fmt.Print(duration)
			fmt.Print(" ")
			total := 0
			for _, instance := range peer.instances {
				for _, block := range instance.blocks {
					for _, tx := range block.txs {
						if !tx.abort {
							total += 1
						}
					}
				}
			}
			//fmt.Println(blockNumber)
			fmt.Println(total)
		}
	}
}

//func CPU_evaluation() {
//	go func() {
//		peer := newPeer(4)
//		peer.run()
//	}()
//	for i := 1; i < 20; i++ {
//		time.Sleep(time.Millisecond * time.Duration(40))
//		percent, _ := cpu.Percent(time.Second, false)
//		fmt.Printf("%v, cpu percent: %v", i, percent)
//	}
//}
