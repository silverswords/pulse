package main

import "fmt"

func main() {
	m := make(map[string]string)
	m[""] = "empty"
	m["haha"] = "?"
	fmt.Println(m[""],m["haha"])
}

