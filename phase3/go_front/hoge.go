package main

import (
	"fmt"
)

func main(){
	var index_pool [][]int
	for i:=0; i <3; i++ {
		index_pool = append(index_pool, make([]int,0))
		for j:=0; j < 3; j++{
			index_pool[i] = append(index_pool[i],i)
		}
	}
	fmt.Print(len(index_pool))
	fmt.Print(len(index_pool[0]))
}
