package main

import(
  "fmt"
)


func main() {
  for {
    go fmt.Printf("1")
    fmt.Printf("0")
  }
}
