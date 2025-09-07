package storage

import "fmt"

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

type SearchTree[T string] struct {
	Children map[string]*SearchTree[T]
	Value    T
}

func NewSearchTree() *SearchTree[string] {
	return &SearchTree[string]{
		Children: make(map[string]*SearchTree[string]),
	}
}

func (s *SearchTree[T]) GetChildValues() []T {
	childValues := []T{}

	for _, c := range s.Children {
		if c.Value != "" {
			childValues = append(childValues, c.Value)
		} else {
			childValues = append(childValues, c.GetChildValues()...)
		}
	}

	return childValues
}

func (s *SearchTree[T]) GetFirstChildValue() T {
	for _, c := range s.Children {
		if c.Value != "" {
			return c.Value
		} else {
			return c.GetFirstChildValue()
		}
	}

	return ""
}

func (s *SearchTree[T]) Get(keys []string) *SearchTree[T] {
	if len(keys) == 0 {
		return s
	}

	if s.Children[keys[0]] == nil {
		return nil
	}

	return s.Children[keys[0]].Get(keys[1:])
}

func (s *SearchTree[T]) Put(keys []string, value T) {
	if len(keys) == 1 {
		s.Children[keys[0]] = &SearchTree[T]{
			Children: make(map[string]*SearchTree[T]),
			Value:    value,
		}
		return
	}

	if s.Children[keys[0]] == nil {
		s.Children[keys[0]] = &SearchTree[T]{
			Children: make(map[string]*SearchTree[T]),
			Value:    "",
		}
	}

	s.Children[keys[0]].Put(keys[1:], value)
}

func (s *SearchTree[T]) Delete(keys []string) (bool, error) {
	if len(keys) == 0 {
		return false, nil
	}

	childNode := s.Children[keys[0]]
	if childNode == nil {
		return false, nil
	}

	if len(keys) == 1 {
		// Base case, delete leaf node

		if childNode.Value == "" {
			// Fatal trying to delete non-leaf node edge case
			return false, fmt.Errorf("trying to delete non-leaf node")
		}

		delete(s.Children, keys[0])
		return len(s.Children) == 0, nil
	}

	isDeletedAndEmpty, err := s.Children[keys[0]].Delete(keys[1:])
	if err != nil {
		return false, err
	}

	if isDeletedAndEmpty {
		delete(s.Children, keys[0])
		return len(s.Children) == 0, nil
	}

	return false, nil
}

func (s *SearchTree[T]) GetPrint() string {
	result := "{"

	if s.Value != "" {
		return string(s.Value)
	}

	for key, val := range s.Children {
		result += key + ": " + val.GetPrint()
	}

	return result + "}"
}
