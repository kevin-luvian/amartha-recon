package storage

import (
	"fmt"
	"reflect"
	"testing"
)

type TestHashableObj struct {
	Id     string
	Date   string
	Source string
}

func (t *TestHashableObj) Hash() (key string, searchKeys []string) {
	return fmt.Sprintf("%s|%s|%s", t.Source, t.Date, t.Id), []string{t.Source, t.Date, t.Id}
}

func HashTable_SetupTestObjects() []TestHashableObj {
	objects := []TestHashableObj{
		{Id: "txn_1", Date: "2025-01-01", Source: "test"},
		{Id: "txn_2", Date: "2025-01-01", Source: "test"},
	}

	return objects
}

func TestHashTable_Put(t *testing.T) {
	root := NewHashTable()
	objects := HashTable_SetupTestObjects()

	for _, obj := range objects {
		root.Put(&obj)
	}

	for _, obj := range objects {
		keyId, _ := obj.Hash()
		foundObj := root.Table[keyId]

		if foundObj == nil {
			t.Fatalf("Expected found %s, got nil", keyId)
		}

		if reflect.DeepEqual(foundObj, obj) {
			t.Fatalf("Expected object %v, got %v", obj, foundObj)
		}
	}
}

func TestHashTable_GetById(t *testing.T) {
	root := NewHashTable()
	objects := HashTable_SetupTestObjects()

	for _, obj := range objects {
		root.Put(&obj)
	}

	for _, obj := range objects {
		keyId, _ := obj.Hash()
		foundObj := root.GetById(keyId)

		if foundObj == nil {
			t.Fatalf("Expected found %s, got nil", keyId)
		}

		if reflect.DeepEqual(foundObj, obj) {
			t.Fatalf("Expected object %v, got %v", obj, foundObj)
		}
	}
}

func TestHashTable_GetFirstMatchByPath(t *testing.T) {
	root := NewHashTable()
	objects := HashTable_SetupTestObjects()

	for _, obj := range objects {
		root.Put(&obj)
	}

	searchKeys := []string{objects[0].Source}
	foundObj := root.GetFirstMatchByPath(searchKeys)

	if foundObj == nil {
		t.Fatalf("Expected found %v, got nil", searchKeys)
	}

	if reflect.DeepEqual(foundObj, objects[0]) {
		t.Fatalf("Expected object %v, got %v", objects[0], foundObj)
	}

	foundObj = root.GetFirstMatchByPath([]string{"invalid_key"})
	if foundObj != nil {
		t.Fatalf("Expected nil, got %v", foundObj)
	}
}

func TestHashTable_Remove(t *testing.T) {
	root := NewHashTable()
	objects := HashTable_SetupTestObjects()

	for _, obj := range objects {
		root.Put(&obj)
	}

	for _, obj := range objects {
		root.Remove(&obj)
	}

	tableLength := len(root.Table)
	if tableLength != 0 {
		t.Fatalf("Expected 0, got %d", tableLength)
	}

	searchTreeLength := len(root.SearchTree.Root.Children)
	searchTreeValue := root.SearchTree.Root.Value
	if searchTreeLength != 0 || searchTreeValue != "" {
		t.Fatalf("Expected empty, got %v", root.SearchTree)
	}
}

func TestHashTable_IsPathContainsOneValue(t *testing.T) {
	root := NewHashTable()
	objects := []TestHashableObj{
		{Id: "txn_1", Date: "2025-01-01", Source: "test"},
		{Id: "txn_2", Date: "2025-01-01", Source: "test"},
	}
	for _, obj := range objects {
		root.Put(&obj)
	}

	_, ok := root.IsPathContainsOneValue([]string{"test"})
	if ok {
		t.Fatalf("Expected false, got %v", ok)
	}

	root = NewHashTable()
	objects = []TestHashableObj{
		{Id: "txn_1", Date: "2025-01-01", Source: "test"},
		{Id: "txn_2", Date: "2025-01-01", Source: "test_2"},
	}
	for _, obj := range objects {
		root.Put(&obj)
	}

	foundId, ok := root.IsPathContainsOneValue([]string{"test"})
	if !ok {
		t.Fatalf("Expected true, got %v", ok)
	}

	expectedId, _ := objects[0].Hash()
	if foundId != expectedId {
		t.Fatalf("Expected %v, got %v", expectedId, foundId)
	}
}

func TestHashTable_GetValues(t *testing.T) {
	root := NewHashTable()

	foundObjects := root.GetValues()
	if len(foundObjects) != 0 {
		t.Fatalf("Expected 0, got %d", len(foundObjects))
	}

	objects := HashTable_SetupTestObjects()
	for _, obj := range objects {
		root.Put(&obj)
	}

	foundObjects = root.GetValues()
	if len(foundObjects) != len(objects) {
		t.Fatalf("Expected %d, got %d", len(objects), len(foundObjects))
	}
}
