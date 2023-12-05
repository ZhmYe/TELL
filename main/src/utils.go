package src

import (
	"math/rand"
	"time"
)

func getDegree(DAG [][]int, index int) int {
	// i->j DAG[i][j] = 1
	degree := 0
	abort := 0
	for i := 0; i < len(DAG); i++ {
		if DAG[i][index] == 1 && i != index {
			degree += 1
		}
		if DAG[index][i] == -1 {
			abort += 1
		}
	}
	if abort == len(DAG) {
		return -1
	}
	return degree
}
func TopologicalOrder(DAG [][]int) []int {
	degrees := make([]int, len(DAG))
	for i, _ := range degrees {
		degrees[i] = getDegree(DAG, i)
	}
	sortResult := make([]int, 0)
	visited := make(map[int]bool, 0)
	for k := 0; k < len(DAG); k++ {
		for i := 0; i < len(DAG); i++ {
			_, flag := visited[i]
			if flag {
				continue
			}
			if degrees[i] == 0 {
				sortResult = append(sortResult, i)
				visited[i] = true
				for j := 0; j < len(DAG); j++ {
					if DAG[i][j] == 1 {
						degrees[j] -= 1
					}
				}
				break
			}
		}
	}
	//fmt.Println(len(sortResult))
	return sortResult
}

func generateRandomAddress() string {
	n := 16
	var letters = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
	result := make([]byte, n)
	rand.Seed(time.Now().Unix())
	for i := range result {
		result[i] = letters[rand.Intn(len(letters))]
	}
	return string(result)

}

func generateRandomTxhash() string {
	n := 16
	var letters = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
	result := make([]byte, n)
	rand.Seed(time.Now().Unix())
	for i := range result {
		result[i] = letters[rand.Intn(len(letters))]
	}
	return string(result)

}
func checkInPath(n int, path []int) bool {
	for _, p := range path {
		if p == n {
			return true
		}
	}
	return false
}
func findCycle(graph Graph, target int, index int, path []int, result *[][]int) {
	if graph[index][target] == 1 {
		tmp := sort(append(path, index))
		exist := false
		for _, r := range *result {
			if len(r) == len(tmp) {
				same := true
				for k, _ := range r {
					if r[k] != tmp[k] {
						same = false
						break
					}
				}
				exist = same
			}
			if exist {
				break
			}
		}
		if !exist {
			*result = append(*result, tmp)
		}
	} else {
		for i, _ := range graph[index] {
			if graph[index][i] == 1 && !checkInPath(i, path) {
				findCycle(graph, target, i, append(path, index), result)
			}
		}
	}
}
func findCycles(graph Graph) [][]int {
	results := make([][]int, 0)
	for i, _ := range graph {
		findCycle(graph, i, i, *new([]int), &results)
	}
	return results
}
func sort(a []int) []int {
	for i := 0; i < len(a); i++ {
		for j := i + 1; j < len(a); j++ {
			if a[i] > a[j] {
				a[i], a[j] = a[j], a[i]
			}
		}
	}
	return a
}
func getMaxFromCounter(m map[int]int) int {
	maxCount := 0
	maxid := -1
	for txid, count := range m {
		if count > maxCount {
			maxCount = count
			maxid = txid
		}
	}
	return maxid
}
func checkStillCycle(m map[int]bool) bool {
	for _, flag := range m {
		if !flag {
			return false
		}
	}
	return true
}

func computeCascade(acgs []ACG) (map[string]map[string][]Unit, map[string]int) {
	// nextReadNumberInAddress Count the number of read operations directly connected after each address for each transaction
	nextReadNumberInAddress := make(map[string]map[string][]Unit, 0) // map[tx_hash] -> map[address] nextReadSet
	for i, acg := range acgs {
		//Traverse each address in ACG to obtain the corresponding stateset
		//The tx of the last unit of each stateset's writeset_ Hash ->map [address] ->length of the readset in the next stateset
		//The next stateset is obtained by continuing to search for the key in the subsequent ACG. If there is no key, it is searched backwards
		//We need to consider determining tx in the future_ Does hash appear after address
		for address, stateset := range acg {
			writeSet := stateset.WriteSet
			tmpDistance := 0
			if len(writeSet) == 0 {
				continue
			}
			lastElement := writeSet[len(writeSet)-1] // lastElement.tx_hash
			// It may be concurrent within a block that has already been aborted, and we need to retrieve the last one that has not been aborted
			tmpFlag := true
			for {
				if lastElement.tx.abort {
					tmpDistance += 1
					if len(writeSet)-1-tmpDistance < 0 {
						tmpFlag = false
						break
					}
					lastElement = writeSet[len(writeSet)-1-tmpDistance]
				} else {
					break
				}
			}
			// All write sets under this address have been aborted, so there is no need for further discussion
			if !tmpFlag {
				continue
			}
			_, inMap := nextReadNumberInAddress[lastElement.tx.txHash]
			if !inMap {
				nextReadNumberInAddress[lastElement.tx.txHash] = make(map[string][]Unit, 0)
			}
			flag := false
			for j := i + 1; j < len(acgs); j++ {
				nextStateSet, exist := acgs[j][address]
				// If the next hashtable contains an address, record its read set length and end it
				if exist {
					nextReadNumberInAddress[lastElement.tx.txHash][address] = nextStateSet.ReadSet
					flag = true
					break
				}
			}
			// If there is no subsequent hashtable
			if !flag {
				nextReadNumberInAddress[lastElement.tx.txHash][address] = make([]Unit, 0)
			}
		}
	}
	//record := nextReadNumberInAddress
	// end for nextReadNumberInAddress
	cascade := make(map[string]int, 0)
	// Calculate the cascade of the first read set on each address of the instance, and add up all addresses in nextReadNumberInAddress for all transactions
	inRecord := make(map[string]bool) // Determine if it is the first read set of each address
	for _, hashtable := range acgs {
		for address, stateset := range hashtable {
			_, haveRecord := inRecord[address]
			// It is the first read set of each address
			if !haveRecord {
				inRecord[address] = true // flag
				// If it is the first read set, the cascade variable needs to be updated
				cascade[address] = getReadSetNumber(stateset.ReadSet, nextReadNumberInAddress)
			}
		}
	}
	return nextReadNumberInAddress, cascade
}
func getReadSetNumber(readSet []Unit, record map[string]map[string][]Unit) int {
	repeatCheck := make(map[string]bool)
	total := 0
	if len(readSet) == 0 {
		return 0
	}
	for _, unit := range readSet {
		_, repeat := repeatCheck[unit.tx.txHash]
		// If there are two read operations in the same address or if the current transaction has already been aborted, there is no need to double calculate
		if repeat || unit.tx.abort {
			continue
		}
		total += 1
		repeatCheck[unit.tx.txHash] = true
		CascadeInAddress, haveCascade := record[unit.tx.txHash]
		if haveCascade {
			for _, eachReadSet := range CascadeInAddress {
				total += getReadSetNumber(eachReadSet, record)
			}
		}
	}
	return total
}
