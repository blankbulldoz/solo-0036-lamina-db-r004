package engine

import (
	"lamina/internal/catalog"
	"lamina/internal/datum"
)

const (
	maxSavepoints   = 64
	txMemoryLimit   = 16 << 20
	stagedBaseCost  = 96
	undoBaseCost    = 48
	scalarValueCost = 16
)

type savepointMarker struct {
	Name  string
	Depth int
}

// undoEntry captures the private write-set state of one key immediately
// before its first mutation inside the savepoint interval at depth. A nil
// prior means the key was not staged at that moment.
type undoEntry struct {
	depth int
	table string
	key   string
	prior *staged
}

type SavepointInfo struct {
	Name  string `json:"name"`
	Depth int    `json:"depth"`
}

type MemoryUsage struct {
	UsedBytes   int `json:"used_bytes"`
	StagedBytes int `json:"staged_bytes"`
	UndoBytes   int `json:"undo_bytes"`
	LimitBytes  int `json:"limit_bytes"`
}

type SavepointStatus struct {
	Name             string      `json:"name"`
	Depth            int         `json:"depth"`
	ActiveSavepoints int         `json:"active_savepoints"`
	StagedKeys       int         `json:"staged_keys"`
	SavepointLimit   int         `json:"savepoint_limit"`
	Memory           MemoryUsage `json:"memory"`
	RestoredKeys     int         `json:"restored_keys,omitempty"`
}

type SavepointList struct {
	Savepoints []SavepointInfo `json:"savepoints"`
	StagedKeys int             `json:"staged_keys"`
	Memory     MemoryUsage     `json:"memory"`
}

func rowCost(row datum.Row) int {
	cost := stagedBaseCost + scalarValueCost*len(row)
	for _, v := range row {
		if v.Kind == "text" {
			cost += len(v.Text)
		}
	}
	return cost
}

func stagedCost(s staged) int {
	if s.Mutation.Deleted {
		return stagedBaseCost
	}
	return rowCost(s.Mutation.Row)
}

func undoCost(e undoEntry) int {
	if e.prior == nil {
		return undoBaseCost
	}
	return undoBaseCost + stagedCost(*e.prior)
}

func cloneStaged(s staged) staged {
	s.Mutation.Row = datum.Clone(s.Mutation.Row)
	return s
}

func (tx *Transaction) topDepth() int {
	if n := len(tx.savepoints); n > 0 {
		return tx.savepoints[n-1].Depth
	}
	return 0
}

func (tx *Transaction) stagedKeyCount() int {
	count := 0
	for _, mutations := range tx.writes {
		count += len(mutations)
	}
	return count
}

func (tx *Transaction) memoryUsage() MemoryUsage {
	return MemoryUsage{UsedBytes: tx.stagedBytes + tx.undoBytes, StagedBytes: tx.stagedBytes, UndoBytes: tx.undoBytes, LimitBytes: txMemoryLimit}
}

func (tx *Transaction) publicSavepoints() []SavepointInfo {
	out := make([]SavepointInfo, 0, len(tx.savepoints))
	for _, sp := range tx.savepoints {
		out = append(out, SavepointInfo{Name: sp.Name, Depth: sp.Depth})
	}
	return out
}

// view returns the externally visible transaction copy without any private
// map state, augmented with live savepoint and resource facts.
func (tx *Transaction) view() Transaction {
	out := *tx
	out.writes = nil
	out.savepoints = nil
	out.undo = nil
	out.undoDepth = nil
	out.StagedKeys = tx.stagedKeyCount()
	out.Savepoints = tx.publicSavepoints()
	out.Memory = tx.memoryUsage()
	return out
}

func (tx *Transaction) savepointStatus(name string, depth, restored int) SavepointStatus {
	return SavepointStatus{Name: name, Depth: depth, ActiveSavepoints: len(tx.savepoints), StagedKeys: tx.stagedKeyCount(), SavepointLimit: maxSavepoints, Memory: tx.memoryUsage(), RestoredKeys: restored}
}

// stage admits one mutation into the private write set. It captures undo
// state for active savepoints and enforces the per-transaction memory budget
// before touching any retained state, so a rejected mutation leaves the
// write set and every savepoint exactly as they were.
func (tx *Transaction) stage(table, key string, next staged) error {
	token := table + "\x00" + key
	current, exists := tx.writes[table][key]
	if !exists {
		if err := tx.reserve(table, key); err != nil {
			return err
		}
	}
	stagedDelta := stagedCost(next)
	if exists {
		stagedDelta -= stagedCost(current)
	}
	addedUndo := 0
	var entry *undoEntry
	if depth := tx.topDepth(); depth > 0 {
		if recorded, ok := tx.undoDepth[token]; !ok || recorded < depth {
			e := undoEntry{depth: depth, table: table, key: key}
			if exists {
				prior := cloneStaged(current)
				e.prior = &prior
			}
			addedUndo = undoCost(e)
			entry = &e
		}
	}
	if tx.stagedBytes+stagedDelta+tx.undoBytes+addedUndo > txMemoryLimit {
		return fault("capacity", "transaction memory limit %d bytes reached (staged %d, undo %d); retained write set and savepoints are unchanged", txMemoryLimit, tx.stagedBytes, tx.undoBytes)
	}
	if entry != nil {
		tx.undo = append(tx.undo, *entry)
		tx.undoDepth[token] = entry.depth
		tx.undoBytes += addedUndo
	}
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	tx.writes[table][key] = next
	tx.stagedBytes += stagedDelta
	return nil
}

func (tx *Transaction) recomputeBytes() {
	tx.stagedBytes = 0
	for _, mutations := range tx.writes {
		for _, s := range mutations {
			tx.stagedBytes += stagedCost(s)
		}
	}
	tx.undoBytes = 0
	tx.undoDepth = map[string]int{}
	for _, e := range tx.undo {
		tx.undoBytes += undoCost(e)
		token := e.table + "\x00" + e.key
		if e.depth > tx.undoDepth[token] {
			tx.undoDepth[token] = e.depth
		}
	}
}

func (tx *Transaction) findSavepoint(name string) int {
	for i, sp := range tx.savepoints {
		if sp.Name == name {
			return i
		}
	}
	return -1
}

func (d *DB) CreateSavepoint(id, name string) (SavepointStatus, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return SavepointStatus{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return SavepointStatus{}, err
	}
	if tx.ReadOnly {
		return SavepointStatus{}, fault("conflict", "read-only transaction cannot create savepoints")
	}
	if !catalog.ValidName(name) {
		return SavepointStatus{}, fault("invalid", "savepoint name must match %s", catalog.NamePattern())
	}
	if tx.findSavepoint(name) >= 0 {
		return SavepointStatus{}, fault("conflict", "savepoint %q already exists; release or roll back to it before recreating", name)
	}
	if len(tx.savepoints) >= maxSavepoints {
		return SavepointStatus{}, fault("capacity", "savepoint depth limit %d reached; release a savepoint before creating another", maxSavepoints)
	}
	tx.nextDepth++
	marker := &savepointMarker{Name: name, Depth: tx.nextDepth}
	tx.savepoints = append(tx.savepoints, marker)
	return tx.savepointStatus(marker.Name, marker.Depth, 0), nil
}

func (d *DB) RollbackSavepoint(id, name string) (SavepointStatus, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return SavepointStatus{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return SavepointStatus{}, err
	}
	idx := tx.findSavepoint(name)
	if idx < 0 {
		return SavepointStatus{}, fault("not_found", "savepoint not active in transaction")
	}
	target := tx.savepoints[idx]
	kept := make([]undoEntry, 0, len(tx.undo))
	seen := map[string]bool{}
	restored := 0
	// Entries stay in chronological order; for each key the earliest entry at
	// or below the target depth restores exactly the value visible when the
	// target savepoint was created. Later entries for that key are skipped.
	for _, e := range tx.undo {
		if e.depth < target.Depth {
			kept = append(kept, e)
			continue
		}
		token := e.table + "\x00" + e.key
		if seen[token] {
			continue
		}
		seen[token] = true
		if e.prior == nil {
			delete(tx.writes[e.table], e.key)
			if len(tx.writes[e.table]) == 0 {
				delete(tx.writes, e.table)
			}
		} else {
			if tx.writes[e.table] == nil {
				tx.writes[e.table] = map[string]staged{}
			}
			tx.writes[e.table][e.key] = cloneStaged(*e.prior)
		}
		restored++
	}
	tx.undo = kept
	markers := make([]*savepointMarker, 0, idx+1)
	for _, sp := range tx.savepoints {
		if sp.Depth <= target.Depth {
			markers = append(markers, sp)
		}
	}
	tx.savepoints = markers
	tx.recomputeBytes()
	return tx.savepointStatus(target.Name, target.Depth, restored), nil
}

func (d *DB) ReleaseSavepoint(id, name string) (SavepointStatus, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return SavepointStatus{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return SavepointStatus{}, err
	}
	idx := tx.findSavepoint(name)
	if idx < 0 {
		return SavepointStatus{}, fault("not_found", "savepoint not active in transaction")
	}
	target := tx.savepoints[idx]
	tx.savepoints = append(tx.savepoints[:idx], tx.savepoints[idx+1:]...)
	// Undo entries are retained: earlier surviving savepoints must still be
	// able to roll back through the released interval by depth filtering.
	return tx.savepointStatus(target.Name, target.Depth, 0), nil
}

func (d *DB) ListSavepoints(id string) (SavepointList, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return SavepointList{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return SavepointList{}, err
	}
	return SavepointList{Savepoints: tx.publicSavepoints(), StagedKeys: tx.stagedKeyCount(), Memory: tx.memoryUsage()}, nil
}
