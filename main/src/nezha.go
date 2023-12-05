package src

// NeZha
// Nezha Implementation
type NeZha struct {
	acg   ACG
	rate  float64
	txs   []*Transaction
	order []int
}

func newNeZha(txs []*Transaction) *NeZha {
	nezha := new(NeZha)
	nezha.rate = 0
	nezha.txs = txs
	nezha.acg = *new(ACG)
	nezha.order = make([]int, 0)
	return nezha
}
func (nezha *NeZha) getACG() {
	nezha.acg = getACG(nezha.txs)
}
func (nezha *NeZha) getAbortRate() float64 {
	abort := 0
	for _, tx := range nezha.txs {
		if tx.abort {
			abort += 1
		}
	}
	nezha.rate = float64(abort) / float64(len(nezha.txs))
	return nezha.rate
}

// Transaction Sort
func getMinSeq(sortedRSet []Unit) int {
	minSeq := 100000000
	for _, unit := range sortedRSet {
		if unit.tx.sequence < minSeq {
			minSeq = unit.tx.sequence
		}
	}
	return minSeq
}
func getMaxSeq(sortedRSet []Unit) int {
	maxSeq := -1
	for _, unit := range sortedRSet {
		if unit.tx.sequence > maxSeq {
			maxSeq = unit.tx.sequence
		}
	}
	return maxSeq
}
func getSortedRSet(Rw StateSet) []Unit {
	sortedRSet := make([]Unit, 0)
	for _, unit := range Rw.ReadSet {
		if unit.tx.sequence != -1 {
			sortedRSet = append(sortedRSet, unit)
		}
	}
	return sortedRSet
}
func getSortedWSet(Rw StateSet) []Unit {
	sortedWSet := make([]Unit, 0)
	for _, unit := range Rw.WriteSet {
		if unit.tx.sequence != -1 {
			sortedWSet = append(sortedWSet, unit)
		}
	}
	return sortedWSet
}

// TransactionSort
// Refer to the pseudocode in the original paper of nezha
func (nezha *NeZha) TransactionSort() {
	nezha.getACG()
	initialSeq := 0
	for _, Rw := range nezha.acg {
		maxRead := -1
		writeSeq := -1
		sortedRSet := getSortedRSet(Rw)
		ReadSetTxHash := make(map[string]bool, 0)
		// line 4 - 15
		if len(sortedRSet) == 0 {
			for _, unit := range Rw.ReadSet {
				unit.tx.sequence = initialSeq
				sortedRSet = append(sortedRSet, unit)
				_, exist := ReadSetTxHash[unit.tx.txHash]
				if !exist {
					ReadSetTxHash[unit.tx.txHash] = true
				}
			}
			maxRead = initialSeq
		} else {
			minSeq := getMinSeq(sortedRSet)
			maxSeq := getMaxSeq(sortedRSet)
			maxRead = maxSeq
			for _, unit := range Rw.ReadSet {
				if unit.tx.sequence == -1 {
					unit.tx.sequence = minSeq
					sortedRSet = append(sortedRSet, unit)
				}
				_, exist := ReadSetTxHash[unit.tx.txHash]
				if !exist {
					ReadSetTxHash[unit.tx.txHash] = true
				}
			}
		}
		// line 16 - 19
		sortedWSet := getSortedWSet(Rw)
		for _, unit := range sortedWSet {
			_, exist := ReadSetTxHash[unit.tx.txHash]
			if exist {
				unit.tx.sequence = maxRead + 1
				maxRead += 1
			}
		}
		// line 20 - 24
		for _, unit := range sortedWSet {
			if unit.tx.sequence < maxRead {
				unit.tx.abort = true
			}
		}
		// line 25 - 29
		if len(Rw.ReadSet) == 0 {
			writeSeq = initialSeq
		} else {
			writeSeq = maxRead + 1
		}
		// line 30 - 35
		for _, unit := range Rw.WriteSet {
			if unit.tx.sequence == -1 {
				unit.tx.sequence = writeSeq
				writeSeq += 1
			}
		}
	}
	nezha.Sort()
}
func (nezha *NeZha) Sort() {
	seq := 0
	flag := false
	for {
		for i, tx := range nezha.txs {
			if tx.abort {
				continue
			}
			if tx.sequence == seq {
				flag = true
				nezha.order = append(nezha.order, i)
			}
		}
		if !flag {
			break
		}
		flag = false
		seq++
	}
}
