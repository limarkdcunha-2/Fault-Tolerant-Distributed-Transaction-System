/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
)


func parseTestCases(filePath string) (map[int]TestCase, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("could not open csv file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Skip the header row
	if _, err := reader.Read(); err != nil {
		return nil, fmt.Errorf("could not read header: %w", err)
	}

	testCases := make(map[int]TestCase)
	var currentTestCase *TestCase

	// Regex to parse transactions like "(A, J, 3)" and node lists like "[n1, n2, n3]"
	txRegex := regexp.MustCompile(`\((?P<sender>\w+),\s*(?P<receiver>\w+),\s*(?P<amount>\d+)\)`)
	nodeRegex := regexp.MustCompile(`n(\d+)`)
	failRegex := regexp.MustCompile(`^F\((\d+)\)$`)
	recoverRegex := regexp.MustCompile(`^R\((\d+)\)$`)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading csv record: %w", err)
		}

		// Column 0: Set Number
		if record[0] != "" {
			// If we are starting a new test case, add the previous one to the map.
			if currentTestCase != nil {
				testCases[currentTestCase.SetNumber] = *currentTestCase
			}

			setNum, _ := strconv.Atoi(record[0])
			currentTestCase = &TestCase{SetNumber: setNum}

			// Column 2: Live Nodes (only present on the first line of a set)
			if record[2] != "" {
				matches := nodeRegex.FindAllStringSubmatch(record[2], -1)
				for _, match := range matches {
					nodeID, _ := strconv.Atoi(match[1])
					currentTestCase.LiveNodes = append(currentTestCase.LiveNodes, nodeID)
				}
			}
		}

		// Column 1: Transactions
		if currentTestCase != nil && record[1] != "" {
			txStr := strings.TrimSpace(record[1])

			if matches := failRegex.FindStringSubmatch(txStr); len(matches) > 0 {
				// Handle F(n)
				nodeID, _ := strconv.Atoi(matches[1])
				currentTestCase.Transactions = append(currentTestCase.Transactions, TestTransaction{
					IsFailNode:   true,
					TargetNodeId: nodeID,
				})
			} else if matches := recoverRegex.FindStringSubmatch(txStr); len(matches) > 0 {
				// Handle R(n)
				nodeID, _ := strconv.Atoi(matches[1])
				currentTestCase.Transactions = append(currentTestCase.Transactions, TestTransaction{
					IsRecoverNode: true,
					TargetNodeId:  nodeID,
				})
			} else {
				// Handle regular transaction
				matches := txRegex.FindStringSubmatch(txStr)
				if len(matches) == 4 { // 0: full match, 1: sender, 2: receiver, 3: amount
					amount, _ := strconv.Atoi(matches[3])
					tx := TestTransaction{
						Sender:   matches[1],
						Receiver: matches[2],
						Amount:   amount,
					}
					currentTestCase.Transactions = append(currentTestCase.Transactions, tx)
				}
			}
		}
	}

	// Add the last test case to the map
	if currentTestCase != nil {
		testCases[currentTestCase.SetNumber] = *currentTestCase
	}

	return testCases, nil
}


func getAllTestCases() map[int]TestCase {
	filePath := "test10k.csv"
    // filePath := "CSE535-F25-Project-1-Testcases.csv"
    
	log.Printf("Parsing test cases from: %s\n", filePath)

	testCases, err := parseTestCases(filePath)
	if err != nil {
		log.Fatalf("Failed to parse test cases: %v", err)
	}

	return testCases
}