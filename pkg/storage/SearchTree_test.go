package storage

import (
	"fmt"
	"testing"
)

type TestObj struct {
	Id     string
	Type   string
	Amount int
	Date   string
}

func TestSearchTree_Put(t *testing.T) {
	root := &SearchTree[string]{Root: Trie[string]{Children: make(map[string]*Trie[string])}}

	// Test data
	objects := []*TestObj{
		{
			Date:   "2025-01-01",
			Type:   "debit",
			Id:     "txnid_12345",
			Amount: 5,
		},
		{
			Date:   "2025-01-01",
			Type:   "credit",
			Id:     "txnid_12346",
			Amount: 10,
		},
		{
			Date:   "2025-01-01",
			Type:   "credit",
			Id:     "txnid_12347",
			Amount: 20,
		},
	}

	for _, obj := range objects {
		keys := []string{obj.Date, obj.Type, fmt.Sprintf("%d", obj.Amount)}
		root.Put(keys, obj.Id)
	}

	// Check date length
	if len(root.Root.Children) != 1 {
		t.Fatalf("Expected 1 child at root, got %d", len(root.Root.Children))
	}

	// Traverse to the inserted objects
	for _, obj := range objects {
		node := root.Root.Children[obj.Date].Children[obj.Type].Children[fmt.Sprintf("%d", obj.Amount)]

		if node == nil {
			t.Fatalf("Expected node to be present, got nil")
		}

		if node.Value != obj.Id {
			t.Errorf("Expected object %v, got %v", obj.Id, node.Value)
		}
	}
}

func TestSearchTree_Get(t *testing.T) {
	root := NewSearchTree()

	// Test data
	objects := []map[string]string{
		{"date": "2025-01-01", "type": "debit", "id": "txnid_12345", "amount": "5"},
		{"date": "2025-01-01", "type": "credit", "id": "txnid_12346", "amount": "10"},
		{"date": "2025-01-01", "type": "credit", "id": "txnid_12347", "amount": "20"},
	}

	for _, obj := range objects {
		keys := []string{obj["date"], obj["type"], obj["amount"]}
		root.Put(keys, obj["id"])
	}

	node := root.Get([]string{"2025-01-01"})
	if len(node.Children) != 2 {
		// debit and credit
		t.Fatalf("Expected size 2, got %d", len(node.Children))
	}

	node = root.Get([]string{"2025-01-01", "credit"})
	if len(node.Children) != 2 {
		// txnid_12346 and txnid_12347
		t.Fatalf("Expected size 2, got %d", len(node.Children))
	}

	node = root.Get([]string{"2025-01-01", "debit", "5"})
	if node == nil {
		t.Fatalf("Expected node found, got nil")
	}

	if node.Value != objects[0]["id"] {
		t.Fatalf("Expected object %v, got %v", objects[0]["id"], node.Value)
	}

	node = root.Get([]string{"2025-01-01", "credit", "123"})
	if node != nil {
		// invalid path
		t.Fatalf("Expected nil, got %v", node)
	}
}

func TestSearchTree_Delete(t *testing.T) {
	root := NewSearchTree()

	// Test data
	objects := []map[string]string{
		{"date": "2025-01-01", "type": "debit", "id": "txnid_12345", "amount": "5"},
		{"date": "2025-01-01", "type": "credit", "id": "txnid_12346", "amount": "10"},
		{"date": "2025-01-01", "type": "credit", "id": "txnid_12347", "amount": "20"},
	}

	for _, obj := range objects {
		keys := []string{obj["date"], obj["type"], obj["id"]}
		root.Put(keys, obj["id"])
	}

	// Check if txnid_12345 is deleted
	root.Delete([]string{"2025-01-01", "debit", "txnid_12345"})
	node := root.Get([]string{"2025-01-01", "debit", "txnid_12345"})
	if node != nil {
		t.Errorf("Expected nil, got %v", node)
	}

	// Check if txnid_12346 is deleted
	root.Delete([]string{"2025-01-01", "credit", "txnid_12346"})
	node = root.Get([]string{"2025-01-01", "credit", "txnid_12346"})
	if node != nil {
		t.Errorf("Expected nil, got %v", node)
	}

	// Check if debit node is pruned, while credit node still exists
	node = root.Get([]string{"2025-01-01"})
	if len(node.Children) != 1 {
		t.Errorf("Expected size 1, got %v", len(node.Children))
	}

	// Delete invalid key
	isDeleted, _ := root.Delete([]string{})
	if isDeleted {
		t.Fatalf("Expected false, got %v", isDeleted)
	}
}

func TestSearchTree_GetChildValues(t *testing.T) {
	root := NewSearchTree()

	// Test data
	objects := []map[string]string{
		{"date": "2025-01-01", "type": "debit", "id": "txnid_12345", "amount": "5"},
		{"date": "2025-01-01", "type": "credit", "id": "txnid_12346", "amount": "10"},
		{"date": "2025-01-01", "type": "credit", "id": "txnid_12347", "amount": "20"},
	}

	for _, obj := range objects {
		keys := []string{obj["date"], obj["type"], obj["id"]}
		root.Put(keys, obj["id"])
	}

	expectedValues := make(map[string]bool, len(objects))
	for _, obj := range objects {
		expectedValues[obj["id"]] = true
	}

	values := root.GetChildValues()
	for _, val := range values {
		if !expectedValues[val] {
			t.Fatalf("Expected in %v, got %s", expectedValues, val)
		}
	}
}

func TestSearchTree_GetFirstChildValue(t *testing.T) {
	root := NewSearchTree()

	value := root.GetFirstChildValue([]string{})
	if value != "" {
		t.Fatalf("Expected empty, Got %v", value)
	}

	// Test data
	objects := []map[string]string{
		{"date": "2025-01-01", "type": "debit", "id": "txnid_12345", "amount": "5"},
	}

	for _, obj := range objects {
		keys := []string{obj["date"], obj["type"], obj["id"]}
		root.Put(keys, obj["id"])
	}

	value = root.GetFirstChildValue([]string{})
	expectedValue := objects[0]["id"]
	if value != expectedValue {
		t.Fatalf("Expected %v, Got %v", expectedValue, value)
	}
}

func TestSearchTree_GetPrint(t *testing.T) {
	root := NewSearchTree()

	// Test data
	objects := []map[string]string{
		{"date": "2025-01-01", "type": "debit", "id": "txnid_12345", "amount": "5"},
	}

	for _, obj := range objects {
		keys := []string{obj["date"], obj["type"], obj["id"]}
		root.Put(keys, fmt.Sprintf("%s|%s", obj["date"], obj["id"]))
	}

	value := root.GetPrint()
	expectedValue := "{2025-01-01: {debit: {txnid_12345: 2025-01-01|txnid_12345}}}"
	if value != expectedValue {
		t.Fatalf("Expected %v, Got %v", expectedValue, value)
	}
}
