package src

import (
	"sync"
)

// Harmony
// Harmony Implementation
type Harmony struct {
	acg   ACG
	rate  float64
	txs   []*Transaction
	order []int
	state []string // 所有地址
}

func newHarmony(txs []*Transaction) *Harmony {
	harmony := new(Harmony)
	harmony.rate = 0
	harmony.txs = txs
	harmony.acg = *new(ACG)
	harmony.order = make([]int, 0)
	return harmony
}
func (harmony *Harmony) getACG() {
	harmony.acg = getACG(harmony.txs)
	for key, _ := range harmony.acg {
		harmony.state = append(harmony.state, key)
	}
}

// Bucket 每一个桶实例，用于并行排序
type Bucket struct {
	state    string
	stateSet *StateSet
	acg      ACG
}

func newBucket(state string, stateSet *StateSet, acg ACG) *Bucket {
	b := new(Bucket)
	b.state = state
	b.stateSet = stateSet
	b.acg = acg
	return b
}
func (b *Bucket) Sort() {
	flag := len(b.stateSet.ReadSet) == 0
	// 没有读集的时候，在当前bucket不会出现危险结构
	if !flag {
		// 不能将读集看成一个整体，因为还要考虑id
		for _, unit := range b.stateSet.ReadSet {
			if unit.tx.abort {
				continue
			}
			i := unit.tx.id // i <= j, i < k
			for _, writeUnit := range b.stateSet.WriteSet {
				j := writeUnit.tx.id
				// 不满足i <= j 或者交易已经被abort
				if i > j || writeUnit.tx.abort {
					continue
				}
				abortFlag := false
				for _, op := range writeUnit.tx.Ops {
					if abortFlag {
						break
					}
					// 寻找T_j的读集
					if op.Type == OpRead {
						readState := op.Key
						// 从acg中找到读的状态的WriteSet
						// 如果没有写集
						if len(b.acg[readState].WriteSet) == 0 {
							continue
						}
						// 如果有写集，判断是否出现i < k
						for _, anotherUnit := range b.acg[readState].WriteSet {
							if anotherUnit.tx.abort {
								continue
							}
							k := anotherUnit.tx.id
							if j == k {
								continue
							}
							if i < k {
								//fmt.Println(i, j, k)
								writeUnit.tx.abort = true
								abortFlag = true
								break
							}
						}
					}
				}
			}
		}
	}
}
func (harmony *Harmony) BucketSortInParalleling() {
	//parallelingNumber := int(math.Min(float64(runtime.NumCPU()), float64(len(harmony.acg)))) // 并发粒度
	parallelingNumber := 100
	for i := 0; i < len(harmony.state); i += parallelingNumber {
		var wg4Execution sync.WaitGroup
		wg4Execution.Add(parallelingNumber)
		for j := 0; j < parallelingNumber; j++ {
			go func(index int, bias int, wg4tx *sync.WaitGroup) {
				defer wg4tx.Done()
				if index+bias >= len(harmony.state) {
					return
				}
				state := harmony.state[index+bias]
				stateSet := harmony.acg[state]
				bucket := newBucket(state, &stateSet, harmony.acg)
				bucket.Sort()
			}(i, j, &wg4Execution)
		}
		wg4Execution.Wait()
	}
}
func (harmony *Harmony) TransactionSort() {
	harmony.getACG()
	harmony.BucketSortInParalleling()
}
func (harmony *Harmony) getAbortRate() float64 {
	abort := 0
	for _, tx := range harmony.txs {
		if tx.abort {
			abort += 1
		}
	}
	harmony.rate = float64(abort) / float64(len(harmony.txs))
	return harmony.rate
}
