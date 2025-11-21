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

    // Regex for normal transactions "(A, B, 3)"
    txRegex := regexp.MustCompile(`\((\w+),\s*(\w+),\s*(\d+)\)`)
    // Regex for read-only transactions "(A)"
    readOnlyRegex := regexp.MustCompile(`\((\w+)\)`)
    // Regex for live nodes "[n1, n2, n3]"
    nodeRegex := regexp.MustCompile(`n(\d+)`)

    for {
        record, err := reader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            return nil, fmt.Errorf("error reading csv record: %w", err)
        }

        // --- Column 0: Set Number ---
        if record[0] != "" {
            // Add previous test case before starting a new one
            if currentTestCase != nil {
                testCases[currentTestCase.SetNumber] = *currentTestCase
            }

            setNum, _ := strconv.Atoi(record[0])
            currentTestCase = &TestCase{
                SetNumber:    setNum,
            }

            // Parse live nodes (column 2)
            if len(record) > 2 && record[2] != "" {
                matches := nodeRegex.FindAllStringSubmatch(record[2], -1)
                for _, match := range matches {
                    nodeID, _ := strconv.Atoi(match[1])
                    currentTestCase.LiveNodes = append(currentTestCase.LiveNodes, nodeID)
                }
            }
        }

        // --- Column 1: Transactions ---
        if currentTestCase != nil && len(record) > 1 && record[1] != "" {
            txStr := strings.TrimSpace(record[1])

            switch {
            case txRegex.MatchString(txStr):
                // Normal transaction (A, B, 3)
                m := txRegex.FindStringSubmatch(txStr)
                amount, _ := strconv.Atoi(m[3])
                currentTestCase.Transactions = append(currentTestCase.Transactions, TestTransaction{
                    Sender:   m[1],
                    Receiver: m[2],
                    Amount:   amount,
                })

            case readOnlyRegex.MatchString(txStr):
                // Read-only transaction (A)
                m := readOnlyRegex.FindStringSubmatch(txStr)
                currentTestCase.Transactions = append(currentTestCase.Transactions, TestTransaction{
                    Sender:   m[1],
                    Receiver: m[1],
                    Amount:   0,
                })
            }
        }
    }

    // Add last test case
    if currentTestCase != nil {
        testCases[currentTestCase.SetNumber] = *currentTestCase
    }

    return testCases, nil
}


func getAllTestCases() map[int]TestCase {
	filePath := "test10k.csv"
	// filePath := "testcases3.csv"
	log.Printf("Parsing test cases from: %s\n", filePath)

	testCases, err := parseTestCases(filePath)
	if err != nil {
		log.Fatalf("Failed to parse test cases: %v", err)
	}

	return testCases
}