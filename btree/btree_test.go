package btree

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func createSequentialTree(t *testing.T, count int) *BPlusTree {
	tree := NewBPlusTree()
	for i := 1; i <= count; i++ {
		err := tree.Insert(int64(i), []byte(fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}
	return tree
}

// tests basic insertion and search functionality
func TestInsertAndSearch(t *testing.T) {
	tree := NewBPlusTree()

	testCases := []struct {
		key   int64
		value string
	}{
		{10, "ten"},
		{5, "five"},
		{15, "fifteen"},
		{3, "three"},
		{7, "seven"},
		{18, "eighteen"},
	}

	tree.debugPrint()

	// insert test cases
	for _, tc := range testCases {
		err := tree.Insert(tc.key, []byte(tc.value))
		if err != nil {
			t.Errorf("Failed to insert key %d: %v", tc.key, err)
		}
	}

	// search for expected val
	for _, tc := range testCases {
		record, err := tree.Search(tc.key)
		if err != nil {
			t.Errorf("Failed to find key %d: %v", tc.key, err)
			continue
		}

		if string(record.Value) != tc.value {
			t.Errorf("For key %d, expected value %s, got %s",
				tc.key, tc.value, string(record.Value))
		}
	}

	// search for non existent key
	_, err := tree.Search(999)
	if err == nil {
		t.Error("Expected error when searching for non-existent key")
	}
}

// test range search, should print keys in order
func TestRangeSearch(t *testing.T) {
	tree := NewBPlusTree()

	// insert from 1 -> 100
	for i := 1; i <= 100; i++ {
		err := tree.Insert(int64(i), []byte(fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	tree.debugPrint()

	testCases := []struct {
		name     string
		minKey   int64
		maxKey   int64
		expected int
	}{
		{"valid_range_10_20", 10, 20, 11},
		{"valid_range_50_60", 50, 60, 11},
		{"valid_range_95_100", 95, 100, 6},
		{"valid_range_1_5", 1, 5, 5},
		{"invalid_range_60_50", 60, 50, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fmt.Printf("\nSearching range [%d, %d]\n", tc.minKey, tc.maxKey)
			records, err := tree.RangeSearch(tc.minKey, tc.maxKey)

			if err != nil {
				t.Fatalf("Failed range search [%d, %d]: %v",
					tc.minKey, tc.maxKey, err)
			}

			if len(records) != tc.expected {
				t.Errorf("Range search [%d, %d]: expected %d records, got %d\nReceived records:",
					tc.minKey, tc.maxKey, tc.expected, len(records))
				for _, rec := range records {
					t.Errorf("Key: %d", rec.Key)
				}
			}
		})
	}
}

// tests concurrent insertions and searches
func TestConcurrentOperations(t *testing.T) {
	tree := NewBPlusTree()
	var wg sync.WaitGroup

	// Number of goroutines for each operation type
	const numGoroutines = 10
	const numOperations = 100

	// Launch concurrent insertions
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()

			// Generate different ranges for each goroutine to avoid duplicates
			base := routineID * numOperations
			for j := 0; j < numOperations; j++ {
				key := int64(base + j)
				value := []byte(fmt.Sprintf("value-%d-%d", routineID, j))

				err := tree.Insert(key, value)
				if err != nil {
					t.Errorf("Concurrent insert failed: %v", err)
				}
			}
		}(i)
	}

	// Launch concurrent searches
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()

			base := routineID * numOperations
			for j := 0; j < numOperations; j++ {
				key := int64(base + j)
				record, err := tree.Search(key)

				if err != nil {
					continue // Key might not be inserted yet
				}

				expectedValue := fmt.Sprintf("value-%d-%d", routineID, j)
				if !bytes.Equal(record.Value, []byte(expectedValue)) {
					t.Errorf("Concurrent search: wrong value for key %d", key)
				}
			}
		}(i)
	}

	// Launch concurrent range searches
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()

			base := routineID * numOperations
			for j := 0; j < numOperations/10; j++ { // Fewer range searches
				minKey := int64(base + (j * 10))
				maxKey := minKey + 9

				records, err := tree.RangeSearch(minKey, maxKey)
				if err != nil {
					continue // Range might not be fully populated yet
				}

				// Verify order
				for k := 1; k < len(records); k++ {
					if records[k].Key <= records[k-1].Key {
						t.Errorf("Range search returned unordered results")
					}
				}
			}
		}(i)
	}

	wg.Wait()
}

// tests the tree with a large number of entries
func TestLargeDataset(t *testing.T) {
	tree := NewBPlusTree()
	rand.Seed(time.Now().UnixNano())

	const numEntries = 1000
	keys := make([]int64, numEntries)

	for i := 0; i < numEntries; i++ {
		key := rand.Int63n(1000000)
		value := []byte(fmt.Sprintf("value-%d", key))

		err := tree.Insert(key, value)
		if err != nil {
			continue // Duplicate key, skip
		}
		keys[i] = key
	}

	for i := 0; i < 100; i++ {
		minIdx := rand.Intn(numEntries)
		maxIdx := rand.Intn(numEntries)
		if minIdx > maxIdx {
			minIdx, maxIdx = maxIdx, minIdx
		}

		minKey := keys[minIdx]
		maxKey := keys[maxIdx]

		records, err := tree.RangeSearch(minKey, maxKey)
		if err != nil {
			t.Errorf("Failed range search in large dataset: %v", err)
			continue
		}

		for j := 1; j < len(records); j++ {
			if records[j].Key <= records[j-1].Key {
				t.Errorf("Records not in order in large dataset")
			}
		}
	}
}
