package storage

// Lookup table for searching keys, assume no duplicated transaction id / paths are unique
//
// Flow:
// (1) Put transaction into SearchHash using keys
// (2) Get transaction using keys:
//     (2.1) If keys exist, return the object
//     (2.2) If keys do not exist, return nil
// (3) Delete transaction using keys, can only delete leaf nodes:
//     (3.1) If keys exist, delete the object and prune parent nodes
//     (3.2) If keys do not exist, do nothing
//

type Trie[T string] struct {
	Children map[string]*Trie[T]
	Value    T
}

type SearchTree[T string] struct {
	Root Trie[T]
}

func NewSearchTree() *SearchTree[string] {
	return &SearchTree[string]{
		Root: Trie[string]{
			Children: make(map[string]*Trie[string]),
			Value:    "",
		},
	}
}

func (s *SearchTree[T]) GetChildValues() []T {
	childValues := []T{}
	stack := []*Trie[T]{&s.Root}

	for {
		if len(stack) == 0 {
			break
		}

		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if node.Value != "" {
			childValues = append(childValues, node.Value)
		} else {
			for _, c := range node.Children {
				stack = append(stack, c)
			}
		}
	}

	return childValues
}

func (s *SearchTree[T]) GetFirstChildValue(keys []string) T {
	node := s.Get(keys)
	if node == nil {
		return ""
	}

	for {
		if node.Value != "" {
			return node.Value
		}

		if len(node.Children) == 0 {
			return ""
		}

		for _, c := range node.Children {
			node = c
			break
		}
	}
}

func (s *SearchTree[T]) Get(keys []string) *Trie[T] {
	node := &s.Root
	var ok bool

	for {
		if len(keys) == 0 {
			return node
		}

		node, ok = node.Children[keys[0]]
		if !ok {
			return nil
		}

		keys = keys[1:]
	}
}

func (s *SearchTree[T]) Put(keys []string, value T) {
	node := &s.Root
	var ok bool

	for {
		if len(keys) == 0 {
			node.Value = value
			return
		}

		_, ok = node.Children[keys[0]]
		if !ok {
			node.Children[keys[0]] = &Trie[T]{
				Children: make(map[string]*Trie[T]),
				Value:    "",
			}
		}

		node = node.Children[keys[0]]
		keys = keys[1:]
	}
}

func (s *SearchTree[T]) Delete(keys []string) (bool, error) {
	stack := []*Trie[T]{&s.Root}
	var ok bool

	for _, key := range keys {
		node := stack[len(stack)-1]
		_, ok = node.Children[key]
		if ok {
			stack = append(stack, node.Children[key])
		} else {
			return false, nil
		}
	}

	for {
		if len(stack) == 0 || len(keys) == 0 {
			return false, nil
		}

		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		key := keys[len(keys)-1]
		keys = keys[:len(keys)-1]

		if len(node.Children) > 0 {
			return false, nil
		}

		if len(stack) > 0 {
			node = stack[len(stack)-1]
			delete(node.Children, key)
		} else {
			return false, nil
		}
	}
}

func (s *SearchTree[T]) GetPrint() string {
	return s.printNode(&s.Root)
}

func (s *SearchTree[T]) printNode(node *Trie[T]) string {
	if node.Value != "" {
		return string(node.Value)
	}

	if len(node.Children) == 0 {
		return ""
	}

	result := "{"
	first := true
	for key, child := range node.Children {
		if !first {
			result += ", "
		}
		result += key + ": " + s.printNode(child)
		first = false
	}
	result += "}"

	return result
}
