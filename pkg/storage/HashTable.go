package storage

// SearchTree:
//   2025-01-01:
//     DEBIT:
//       5.00:
//	       txn_1:
//		     2025-01-01|DEBIT|txn_1
//	   CREDIT:
//	     5.00:
//		   txn_2:
// 		     2025-01-01|DEBIT|txn_2
//		   txn_3:
//           2025-01-01|DEBIT|txn_3
//		 6.00:
//		   txn_4:
//		     2025-01-01|DEBIT|txn_4
//
// Table:
// 	   2025-01-01|DEBIT|txn_1: &{ID: txn_1, Amount: 5.00, ...}
//     2025-01-01|CREDIT|txn_2: &{ID: txn_2, Amount: 5.00, ...}
//     2025-01-01|CREDIT|txn_3: &{ID: txn_3, Amount: 5.00, ...}
//     2025-01-01|CREDIT|txn_4: &{ID: txn_4, Amount: 6.00, ...}

type HashTable struct {
	SearchTree *SearchTree[string]
	Table      map[string]IHashable
}

func NewHashTable() *HashTable {
	return &HashTable{
		SearchTree: NewSearchTree(),
		Table:      make(map[string]IHashable),
	}
}

func (h *HashTable) GetById(key string) IHashable {
	return h.Table[key]
}

func (h *HashTable) GetFirstMatchByPath(path []string) IHashable {
	key := h.SearchTree.GetFirstChildValue(path)
	if key == "" {
		return nil
	}

	return h.Table[key]
}

func (h *HashTable) IsPathContainsOneValue(path []string) (string, bool) {
	return h.SearchTree.IsPathContainsOneValue(path)
}

func (h *HashTable) Put(obj IHashable) {
	key, searchKeys := obj.Hash()
	h.Table[key] = obj
	h.SearchTree.Put(searchKeys, key)
}

func (h *HashTable) Remove(obj IHashable) {
	key, searchKeys := obj.Hash()

	delete(h.Table, key)
	h.SearchTree.Delete(searchKeys)
}

func (h *HashTable) GetValues() []IHashable {
	all := make([]IHashable, 0, len(h.Table))
	for _, v := range h.Table {
		all = append(all, v)
	}
	return all
}
