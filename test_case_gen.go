package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

func gen_test_cases() {

	participants := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}

	const sets = 1
	const txPerSet = 10

	f, err := os.Create("test1.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Header
	if err := w.Write([]string{"Set Number", "Transactions", "Live Nodes"}); err != nil {
		panic(err)
	}

	for set := 1; set <= sets; set++ {
		// Random live nodes between 4 and 7
		numLive := 5 // 4-7 inclusive
		live := randomSubset(nodes, numLive)
		liveStr := "[" + strings.Join(live, ", ") + "]"

		for i := 0; i < txPerSet; i++ {
			src := participants[rand.Intn(len(participants))]
			dst := participants[rand.Intn(len(participants))]
			amt := rand.Intn(10) + 1 // 1-10

			tx := fmt.Sprintf("(%s, %s, %d)", src, dst, amt)

			if i == 0 {
				// First transaction in set: include set number and live nodes
				if err := w.Write([]string{strconv.Itoa(set), tx, liveStr}); err != nil {
					panic(err)
				}
			} else {
				// Subsequent transactions in set: blank set number and live nodes
				if err := w.Write([]string{"", tx, ""}); err != nil {
					panic(err)
				}
			}
		}
	}

	// Ensure data is flushed
	w.Flush()
	if err := w.Error(); err != nil {
		panic(err)
	}

	// fmt.Println("Generated transactions_2000.csv with 20 sets x 100 transactions")
}

func randomSubset(src []string, count int) []string {
	if count >= len(src) {
		out := make([]string, len(src))
		copy(out, src)
		return out
	}
	perm := rand.Perm(len(src))[:count]
	out := make([]string, count)
	for i, idx := range perm {
		out[i] = src[idx]
	}
	return out
}
