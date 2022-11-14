package main

import (
	"6.824/mr"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
)

func main() {
	fmt.Println(os.TempDir())
	fmt.Println(os.TempDir())	
	new,_ := filepath.Abs("./mr-1123-0")
	b,_ := filepath.Abs("./mr-0-0")
	err := os.Rename(b,new)
	if err != nil {
		log.Fatal(err)
	}
	return
	//enc := json.NewEncoder(file)
	//fmt.Println(ihash("producedd1")%10)
	var a *int
	//b := 10
	//a = &b
	if a != nil {
		fmt.Println(111)
	}
	file, _ := os.Open("mr-0-1")
	fmt.Println(file)
	dec := json.NewDecoder(file)
	var kva []mr.KeyValue
	for {
		var kv mr.KeyValue
		if err := dec.Decode(&kv); err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(ihash(kv.Key)%10)
		kva = append(kva, kv)
	}
	fmt.Println(kva)
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}