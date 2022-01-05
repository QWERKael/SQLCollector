package util

import "fmt"

type Empty struct{}
type Set struct {
	inner map[string]Empty
}

func NewEmptySet() *Set {
	return &Set{map[string]Empty{}}
}

func NewSet(values []string) *Set {
	s := NewEmptySet()
	for _, value := range values {
		s.Insert(value)
	}
	return s
}

func (s *Set) Insert(key string) {
	s.inner[key] = Empty{}
}

func (s *Set) Del(key string) {
	delete(s.inner, key)
}

func (s *Set) Len() int {
	return len(s.inner)
}

func (s *Set) Clear() {
	s.inner = make(map[string]Empty)
}

func (s *Set) List() []string {
	list := make([]string, 0)
	for k := range s.inner {
		list = append(list, k)
	}
	return list
}

func (s *Set) Exists(key string) bool {
	for k, _ := range s.inner {
		if k == key {
			return true
		}
	}
	return false
}

func (s *Set) Equal(t *Set) bool {
	if s.Len() != t.Len() {
		fmt.Println("长度不等")
		return false
	}
	for k, _ := range s.inner {
		if !t.Exists(k) {
			fmt.Printf("不存在 %s", k)
			return false
		}
	}
	return true
}

func (s *Set) ToStringList() []string {
	list := make([]string, 0)
	for k, _ := range s.inner {
		list = append(list, k)
	}
	return list
}
