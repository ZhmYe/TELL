package src

// Unit Operation unit, a read/write operation for each transaction
type Unit struct {
	op Op           // operate
	tx *Transaction // tx
}

func newUnit(op Op, tx *Transaction) *Unit {
	unit := new(Unit)
	unit.op = op
	unit.tx = tx
	return unit
}

// StateSet

// StateSet line in ACG
type StateSet struct {
	ReadSet  []Unit // Read Set
	WriteSet []Unit // Write Set
}

func newStateSet() *StateSet {
	set := new(StateSet)
	set.ReadSet = make([]Unit, 0)
	set.WriteSet = make([]Unit, 0)
	return set
}
func (stateSet *StateSet) appendToReadSet(unit Unit) {
	stateSet.ReadSet = append(stateSet.ReadSet, unit)
}
func (stateSet *StateSet) appendToWriteSet(unit Unit) {
	stateSet.WriteSet = append(stateSet.WriteSet, unit)
}

// ACG address->StateSet
type ACG = map[string]StateSet

// Build ACG corresponding to concurrent txs
func getACG(txs []*Transaction) ACG {
	acg := make(ACG)
	for _, tx := range txs {
		for _, op := range tx.Ops {
			_, exist := acg[op.Key]

			// If the address does not exist in ACG, create a new StateSet
			if !exist {
				acg[op.Key] = *newStateSet()
			}

			unit := newUnit(*op, tx) // new unit
			stateSet := acg[op.Key]

			// Add to the two parts of the StateSet based on read/write operations
			switch unit.op.Type {
			case OpRead:
				stateSet.appendToReadSet(*unit)
			case OpWrite:
				stateSet.appendToWriteSet(*unit)
			}
			acg[op.Key] = stateSet
		}
	}
	return acg
}
