package btree

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	MinDegree = 2
	MaxDegree = MinDegree * 2
)

type Record struct {
	Key   int64
	Value []byte
}

type Node struct {
	// node level lock
	mu sync.RWMutex

	isLeaf   bool
	keys     []int64
	children []*Node
	records  []Record
	next     *Node
	parent   *Node
}

type BPlusTree struct {
	// tree level lock, we use RWMutex instead of regular Mutex so multiple readers can access the data simultaneously, or a single writer
	mu   sync.RWMutex
	root *Node
}

func NewBPlusTree() *BPlusTree {
	root := &Node{
		isLeaf:  true,
		keys:    make([]int64, 0),
		records: make([]Record, 0),
	}
	return &BPlusTree{
		root: root,
	}
}

// adds a k/v pair to the tree
func (t *BPlusTree) Insert(key int64, value []byte) error {
	fmt.Printf("\nInserting key: %d\n", key)
	t.mu.Lock()
	defer t.mu.Unlock()

	// if the tree is empty, create a new root and add
	if len(t.root.keys) == 0 {
		fmt.Println("Inserting into empty root")
		t.root.keys = append(t.root.keys, key)
		t.root.records = append(t.root.records, Record{Key: key, Value: value})
		t.debugPrint()
		return nil
	}

	// if root is full, create new root
	if len(t.root.keys) == MaxDegree-1 {
		fmt.Println("Root is full, creating new root")
		newRoot := &Node{
			isLeaf:   false,
			keys:     make([]int64, 0),
			children: []*Node{t.root},
		}
		t.root.parent = newRoot
		t.root = newRoot
		t.root.splitChild(0)
	}

	// continue with insertion
	err := t.insertNonFull(t.root, key, value)
	fmt.Println("After insertion:")
	t.debugPrint()
	return err
}

// inserts a kv pair onto a non full node
func (t *BPlusTree) insertNonFull(node *Node, key int64, value []byte) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	fmt.Printf("Inserting into node. IsLeaf: %v, Keys: %v\n", node.isLeaf, node.keys)

	// once we arrive at a leaf node, find insert pos and insert
	if node.isLeaf {
		insertPos := 0
		for insertPos < len(node.keys) && key > node.keys[insertPos] {
			insertPos++
		}

		if insertPos < len(node.keys) && node.keys[insertPos] == key {
			fmt.Printf("Updating existing key %d\n", key)
			node.records[insertPos].Value = value
			return nil
		}

		// shift data to make room and insert
		fmt.Printf("Inserting at position %d in leaf\n", insertPos)
		node.keys = append(node.keys, 0)
		node.records = append(node.records, Record{})
		copy(node.keys[insertPos+1:], node.keys[insertPos:])
		copy(node.records[insertPos+1:], node.records[insertPos:])
		node.keys[insertPos] = key
		node.records[insertPos] = Record{Key: key, Value: value}
		return nil
	}

	// else we are at an internal node and need to recurse down
	childIndex := 0
	for childIndex < len(node.keys) && key >= node.keys[childIndex] {
		childIndex++
	}

	fmt.Printf("Selected child index: %d\n", childIndex)
	child := node.children[childIndex]

	// if the child we are recursing down on is going to be full with this addition, split THEN recurse, so we can propogate the key up
	if len(child.keys) == MaxDegree-1 {
		fmt.Println("Child is full, splitting")
		node.splitChild(childIndex)
		if key >= node.keys[childIndex] {
			childIndex++
		}
		child = node.children[childIndex]
	}

	// recursive step
	return t.insertNonFull(child, key, value)
}

func (t *BPlusTree) debugPrint() {
	fmt.Println("Tree structure:")
	t.root.debugPrint(0)
}

func (n *Node) debugPrint(level int) {
	indent := strings.Repeat("  ", level)
	fmt.Printf("%sKeys: %v\n", indent, n.keys)
	if !n.isLeaf {
		for _, child := range n.children {
			child.debugPrint(level + 1)
		}
	}
}

func (n *Node) splitChild(childIndex int) {
	// get child node to be split
	child := n.children[childIndex]

	newNode := &Node{
		isLeaf: child.isLeaf,
		keys:   make([]int64, 0),
		parent: n,
	}

	// if the child is a leaf node, find midpoint then append and truncate kv pairs accordingly
	if child.isLeaf {
		mid := len(child.keys) / 2

		newNode.keys = append(newNode.keys, child.keys[mid:]...)
		newNode.records = make([]Record, 0)
		newNode.records = append(newNode.records, child.records[mid:]...)

		child.keys = child.keys[:mid]
		child.records = child.records[:mid]

		newNode.next = child.next
		child.next = newNode

		// promote the first key of the right node, newNode = right, child = left
		promoteKey := newNode.keys[0]

		// insert the key into the parent
		n.keys = append(n.keys, 0)
		copy(n.keys[childIndex+1:], n.keys[childIndex:])
		n.keys[childIndex] = promoteKey

		n.children = append(n.children, nil)
		copy(n.children[childIndex+2:], n.children[childIndex+1:])
		n.children[childIndex+1] = newNode

		fmt.Printf("Split leaf node: child keys=%v, new node keys=%v, parent keys=%v\n",
			child.keys, newNode.keys, n.keys)
	} else {
		// for internal nodes, we just move the keys and child pointers, also we have to update the parent pointers of the new right node
		mid := len(child.keys) / 2

		newNode.keys = append(newNode.keys, child.keys[mid+1:]...)
		promoteKey := child.keys[mid]
		child.keys = child.keys[:mid]

		newNode.children = make([]*Node, 0)
		newNode.children = append(newNode.children, child.children[mid+1:]...)
		child.children = child.children[:mid+1]

		for _, movedChild := range newNode.children {
			movedChild.parent = newNode
		}

		n.keys = append(n.keys, 0)
		copy(n.keys[childIndex+1:], n.keys[childIndex:])
		n.keys[childIndex] = promoteKey

		n.children = append(n.children, nil)
		copy(n.children[childIndex+2:], n.children[childIndex+1:])
		n.children[childIndex+1] = newNode

		fmt.Printf("Split internal node: child keys=%v, new node keys=%v, parent keys=%v\n",
			child.keys, newNode.keys, n.keys)
	}
}

// search down til we find the kv pair
func (t *BPlusTree) Search(key int64) (*Record, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.root == nil {
		return nil, errors.New("tree is empty")
	}

	node := t.root
	// traverse down
	for !node.isLeaf {
		node.mu.RLock()

		i := 0
		for i < len(node.keys) && key >= node.keys[i] {
			i++
		}

		child := node.children[i]
		node.mu.RUnlock()
		node = child
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	for i := 0; i < len(node.keys); i++ {
		if node.keys[i] == key {
			return &node.records[i], nil
		}
	}
	return nil, errors.New("key not found")
}

// return all kv pairs btwn and inclusive of min and max
func (t *BPlusTree) RangeSearch(minKey, maxKey int64) ([]Record, error) {
	if minKey > maxKey {
		return []Record{}, nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.root == nil {
		return nil, errors.New("tree is empty")
	}
	// search to find leaf node with minKey
	node := t.root
	for {
		node.mu.RLock()

		if node.isLeaf {
			break
		}

		i := 0
		for i < len(node.keys) && minKey >= node.keys[i] { // Changed > to >=
			i++
		}
		child := node.children[i]
		node.mu.RUnlock()
		node = child
	}

	result := make([]Record, 0)
	// traverse leaf nodes til we exceed maxKey
	for node != nil {
		found := false
		for i, key := range node.keys {
			if key > maxKey {
				node.mu.RUnlock()
				return result, nil
			}

			if key >= minKey {
				found = true
				result = append(result, node.records[i])
			}
		}
		next := node.next
		node.mu.RUnlock()
		if next == nil || (found && next.keys[0] > maxKey) {
			break
		}
		next.mu.RLock()
		node = next
	}
	return result, nil
}
