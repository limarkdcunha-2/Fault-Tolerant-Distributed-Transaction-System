/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
)

type BankAccount struct {
    Name string `json:"name"`
    Balance int32 `json:"balance"`
}

type BankAccounts struct {
    Accounts []BankAccount `json:"accounts"`
}


func (node *Node) processTransaction(transaction Transaction) (bool, error) {
    sender := transaction.Sender
    receiver := transaction.Receiver
    amount := transaction.Amount

    if amount <= 0 {
        return false, fmt.Errorf("amount must be positive")
    }

    // Read from the in-memory map
    senderBalance, ok := node.state[sender]
    if !ok {
        return false, fmt.Errorf("sender '%s' not found", sender)
    }
    if _, ok := node.state[receiver]; !ok {
        return false, fmt.Errorf("receiver '%s' not found", receiver)
    }

    // Check balance
    if senderBalance < amount {
        return false, fmt.Errorf("insufficient funds for %s", sender)
    }

    // Execute transfer IN MEMORY
    node.state[sender] -= amount
    node.state[receiver] += amount

    return true, nil
}


func loadInitialState() BankAccounts {
    data := BankAccounts{
		Accounts: []BankAccount{
			{Name: "A", Balance: 10},
			{Name: "B", Balance: 10},
			{Name: "C", Balance: 10},
			{Name: "D", Balance: 10},
			{Name: "E", Balance: 10},
			{Name: "F", Balance: 10},
			{Name: "G", Balance: 10},
			{Name: "H", Balance: 10},
			{Name: "I", Balance: 10},
			{Name: "J", Balance: 10},
		},
	}
    
    return data
}

func (node *Node) loadState() error {
    // This is the file-reading logic from your processTransaction
    jsonFilename := fmt.Sprintf("state_node%d.json", node.nodeId)
    jsonFilePath := "state/" + jsonFilename
    var bankAccounts BankAccounts

    if _, err := os.Stat(jsonFilePath); os.IsNotExist(err) {
        log.Printf("[Node %d] No state file found, loading initial state.", node.nodeId)
        bankAccounts = loadInitialState() // You already have this function
        
        // Write it to disk so it exists
        if err := writeToJson(jsonFilePath, bankAccounts); err != nil {
            return fmt.Errorf("failed to write initial state: %v", err)
        }
    } else {
        log.Printf("[Node %d] Loading existing state from %s", node.nodeId, jsonFilePath)
        byteValue := readFromJson(jsonFilePath) // You have this
        if err := json.Unmarshal(byteValue, &bankAccounts); err != nil {
            return fmt.Errorf("failed to unmarshal existing state: %v", err)
        }
    }

    node.muState.Lock()
    defer node.muState.Unlock()
    
    for _, acc := range bankAccounts.Accounts {
        node.state[acc.Name] = acc.Balance
    }
    
    return nil
}

//  Will be called inside a muStateLock
func (node *Node) persistState() { 
    var bankAccounts BankAccounts
    for name, balance := range node.state {
        bankAccounts.Accounts = append(bankAccounts.Accounts, BankAccount{Name: name, Balance: balance})
    }
    
    sort.Slice(bankAccounts.Accounts, func(i, j int) bool {
        return bankAccounts.Accounts[i].Name < bankAccounts.Accounts[j].Name
    })

    jsonFilename := fmt.Sprintf("state_node%d.json", node.nodeId)
    jsonFilePath := "state/" + jsonFilename
    
    if err := writeToJson(jsonFilePath, bankAccounts); err != nil {
        log.Printf("[Node %d] ERROR: Failed to persist state to disk: %v", node.nodeId,err)
    } else {
        log.Printf("[Node %d] Successfully persisted state to %s", node.nodeId, jsonFilePath)
    }
}


func (node *Node) DeleteStateFile() error {
    jsonFilename := fmt.Sprintf("state_node%d.json", node.nodeId) // e.g., state_node3.json [attached_file:9]
    jsonFilePath := "state/" + jsonFilename

    if err := os.Remove(jsonFilePath); err != nil {
        if os.IsNotExist(err) {
            log.Printf("[Node %d] State file already absent: %s", node.nodeId, jsonFilePath) // ok [attached_file:9]
            return nil
        }
        return fmt.Errorf("[Node %d] failed to delete %s: %v", node.nodeId, jsonFilePath, err) // [attached_file:9]
    }

    log.Printf("[Node %d] Deleted state file: %s", node.nodeId, jsonFilePath) // [attached_file:9]
    return nil
}


func readFromJson(jsonFilename string) []byte {
    jsonFile, err := os.Open(jsonFilename)

	if err != nil {
		log.Println("Error opening file:", err)
        return []byte{}
	}

	defer jsonFile.Close()

    byteValue, err := io.ReadAll(jsonFile)

    if err != nil {
        log.Printf("Error reading file %s: %v\n", jsonFilename, err)
        return []byte{}
    }

    return byteValue
}


func writeToJson(filename string, data interface{}) error {
    if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
        return fmt.Errorf("failed to create directory: %w", err)
    }

	updatedJsonData, err := json.MarshalIndent(data, "", "  ")

	if err != nil {
		return fmt.Errorf("error marshaling JSON: %w", err)
	}

	err = os.WriteFile(filename, updatedJsonData, 0644)
	if err != nil {
		return fmt.Errorf("error writing to file: %w", err)
	}

	return nil
}