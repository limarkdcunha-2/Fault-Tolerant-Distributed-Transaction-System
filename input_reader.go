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
	txRegex := regexp.MustCompile(`^\((\w+),\s*(\w+),\s*(\d+)\)$`)
	
	// Match read-only: (datapoint) - single value in parentheses
	readOnlyRegex := regexp.MustCompile(`^\((\w+)\)$`)
	
	// Match fail command: F(n5) or F(n2)
	failRegex := regexp.MustCompile(`^F\(n(\d+)\)$`)
	
	// Match recover command: R(n5) or R(n2)
	recoverRegex := regexp.MustCompile(`^R\(n(\d+)\)$`)
	
	// Match node list: [n1, n2, n4, n5, n7, n8]
	nodeRegex := regexp.MustCompile(`n(\d+)`)
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

				if readOnlyRegex.MatchString(txStr){
					matches := readOnlyRegex.FindStringSubmatch(txStr)

					currentTestCase.Transactions = append(currentTestCase.Transactions, TestTransaction{
						Sender:   matches[1],
						Receiver: matches[1],
						Amount:   0,
					})
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
	// filePath := "test20.csv"
    // filePath := "CSE535-F25-Project-1-Testcases.csv"
	filePath := "CSE535-F25-Project-3-Testcases.csv"
    
	log.Printf("Parsing test cases from: %s\n", filePath)

	testCases, err := parseTestCases(filePath)
	if err != nil {
		log.Fatalf("Failed to parse test cases: %v", err)
	}

	return testCases
}