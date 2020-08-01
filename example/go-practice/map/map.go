package main

import "fmt"

func main() {
	var drivers = make(map[string]string)
	driveri, ok := drivers["hello"]
	fmt.Println(driveri, ok)
	drivers["hello"] = "world"
	driveri, ok = drivers["hello"]
	fmt.Println(driveri, ok)
}
