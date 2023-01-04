package memdb

import (
	"bytes"
	"math"
)

type ListNode struct {
	Prev *ListNode
	Next *ListNode
	Val  []byte
}

type List struct {
	// dummy head and dummy tail
	Head *ListNode
	Tail *ListNode
	// because of the dummy head and tail,the num of nodes is length + 2
	Length int
}

func NewList() *List {
	// dummy head and tail
	head := &ListNode{}
	tail := &ListNode{}
	head.Next = tail
	tail.Prev = head
	list := &List{
		Head:   head,
		Tail:   tail,
		Length: 0,
	}
	return list
}

func (list *List) index(index int) []byte {

	if index < 0 {
		if -index > list.Length {
			return nil
		}
		currentNode := list.Tail.Prev
		count := -1
		for count > index {
			count--
			currentNode = currentNode.Prev
		}
		return currentNode.Val
	} else {
		if index >= list.Length {
			return nil
		}
		currentNode := list.Head.Next
		count := 0
		for count < index {
			count++
			currentNode = currentNode.Next
		}
		return currentNode.Val
	}

}

// if isBefore is true,insert the node before the target
// else insert it after the target
func (list *List) lInsert(isBefore bool, val []byte, target []byte) int {
	for currentNode := list.Head.Next; currentNode != list.Tail; currentNode = currentNode.Next {
		if bytes.Equal(currentNode.Val, target) {
			newNode := &ListNode{}
			newNode.Val = val
			if isBefore {
				newNode.Prev = currentNode.Prev
				currentNode.Prev.Next = newNode
				newNode.Next = currentNode
				currentNode.Prev = newNode
			} else {
				newNode.Next = currentNode.Next
				currentNode.Next.Prev = newNode
				newNode.Prev = currentNode
				currentNode.Next = newNode
			}
			list.Length++
			return list.Length
		}
	}
	return -1
}

// only push a val call lPush multiple times in list.go to push multiple vals
// don't forget about the dummy head
func (list *List) lPush(val []byte) int {
	newNode := &ListNode{}
	newNode.Val = val
	newNode.Next = list.Head.Next
	list.Head.Next.Prev = newNode
	list.Head.Next = newNode
	newNode.Prev = list.Head
	list.Length++
	return list.Length
}
func (list *List) lPop() []byte {
	if list.Length == 0 {
		return nil
	}
	temp := list.Head.Next
	list.Head.Next = temp.Next
	temp.Next.Prev = list.Head
	temp.Prev = nil
	temp.Next = nil
	list.Length--
	return temp.Val
}

func (list *List) rPush(val []byte) int {
	newNode := &ListNode{}
	newNode.Val = val
	list.Tail.Prev.Next = newNode
	newNode.Prev = list.Tail.Prev
	newNode.Next = list.Tail
	list.Tail.Prev = newNode
	list.Length++
	return list.Length
}
func (list *List) rPop() []byte {
	temp := list.Tail.Prev
	temp.Prev.Next = list.Tail
	list.Tail.Prev = temp.Prev
	temp.Prev = nil
	temp.Next = nil
	list.Length--
	return temp.Val
}

// when lPos return -1 it means can not find the val,
// in list.go,return nil to the redis-cli
// support param rank and maxLen
func (list *List) lPos(val []byte, rank int, maxLen int) int {
	rankCount := 1
	lenCount := 0
	for currentNode := list.Head.Next; currentNode != list.Tail; currentNode = currentNode.Next {
		if bytes.Equal(currentNode.Val, val) {
			if rankCount == rank {
				return lenCount
			} else {
				rankCount++
			}
		}
		lenCount++
		if lenCount >= maxLen {
			return -1
		}

	}
	return -1
}

// when the length of result is 0,list.go return "empty list or set" to redis-cli
func (list *List) lRange(start int, end int) [][]byte {
	if start < 0 {
		start = list.Length + start
	}
	if end < 0 {
		end = list.Length + end
	}
	if start > end || start >= list.Length || end < 0 {
		return nil
	}
	if start < 0 {
		start = 0
	}
	if end >= list.Length {
		end = list.Length - 1
	}
	result := make([][]byte, 0)
	currentNode := list.Head.Next
	for i := 0; i <= end; i++ {
		if i >= start {
			result = append(result, currentNode.Val)
		}
		currentNode = currentNode.Next
	}
	return result
}

func (list *List) deleteNode(node *ListNode) {
	prev := node.Prev
	next := node.Next
	prev.Next = next
	next.Prev = prev
	node.Next = nil
	node.Prev = nil
	list.Length--

}
func (list *List) deleteByVal(val []byte, count int) int {
	deleteNums := 0
	if count == 0 {
		for currentNode := list.Head.Next; currentNode != list.Tail; currentNode = currentNode.Next {
			if bytes.Equal(currentNode.Val, val) {
				tempNext := currentNode.Next
				list.deleteNode(currentNode)
				currentNode = tempNext
				deleteNums++
			}
		}
	} else if count > 0 {
		for currentNode := list.Head.Next; currentNode != list.Tail; currentNode = currentNode.Next {
			if bytes.Equal(currentNode.Val, val) {
				tempNext := currentNode.Next
				list.deleteNode(currentNode)
				currentNode = tempNext
				deleteNums++
				if deleteNums >= count {
					break
				}
			}
		}
	} else if count < 0 {
		for currentNode := list.Tail.Prev; currentNode != list.Head; currentNode = currentNode.Prev {
			if bytes.Equal(currentNode.Val, val) {
				tempPrev := currentNode.Prev
				list.deleteNode(currentNode)
				currentNode = tempPrev
				deleteNums--
				if deleteNums <= count {
					break
				}
			}
		}
	}
	return int(math.Abs(float64(deleteNums)))
}
func (list *List) set(val []byte, index int) bool {
	if index >= 0 {

		currentNode := list.Head.Next
		for i := 0; i <= index; i++ {
			if currentNode == list.Tail {
				return false
			}
			if i == index {
				currentNode.Val = val
				return true
			}
			currentNode = currentNode.Next
		}
	} else {
		currentNode := list.Tail.Prev
		for i := -1; i >= index; i-- {
			if currentNode == list.Head {
				return false
			}
			if i == index {
				currentNode.Val = val
				return true
			}
			currentNode = currentNode.Prev
		}
	}
	return false
}

func (list *List) trim(start int, end int) {
	if list.Length == 0 {
		return
	}
	if start < 0 {
		start = start + list.Length
	}
	if end < 0 {
		end = end + list.Length
	}
	if start > end || start > list.Length || end < 0 {
		list.Clear()
		return
	}
	if start < 0 {
		start = 0
	}
	if end >= list.Length {
		end = list.Length - 1
	}
	var startNode *ListNode
	var endNode *ListNode
	count := 0
	for currentNode := list.Head.Next; currentNode != list.Tail; currentNode = currentNode.Next {
		if count == start {
			startNode = currentNode
		}
		if count == end {
			endNode = currentNode
		}
		count++
	}
	// break the list for gc and link the newList
	list.Head.Next.Prev = nil
	list.Tail.Prev.Next = nil
	if startNode.Prev != nil {
		startNode.Prev.Next = nil
	}
	if endNode.Next != nil {
		endNode.Next.Prev = nil
	}
	list.Head.Next = startNode
	startNode.Prev = list.Head
	list.Tail.Prev = endNode
	endNode.Next = list.Tail
	list.Length = end - start + 1

}

func (list *List) Clear() {
	if list.Length == 0 {
		return
	}
	first := list.Head.Next
	last := list.Tail.Prev
	list.Head.Next = list.Tail
	list.Tail.Prev = list.Head
	list.Length = 0

	first.Prev = nil
	last.Next = nil
}
