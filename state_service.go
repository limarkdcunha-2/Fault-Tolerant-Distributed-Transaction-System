/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/cockroachdb/pebble"
)

type BankAccount struct {
    Name string `json:"name"`
    Balance int32 `json:"balance"`
}

type BankAccounts struct {
    Accounts []BankAccount `json:"accounts"`
}

// Has to be called under muExec
func (node *Node) processIntraShardTransaction(transaction Transaction) (bool, error) {
    sender := transaction.Sender
    receiver := transaction.Receiver
    amount := transaction.Amount
    
    if sender == receiver {
        return false, fmt.Errorf("sender and receiver should be distinct")
    }
    
    if amount <= 0 {
        return false, fmt.Errorf("amount must be positive")
    }

    // log.Printf("[Node %d] Amt check complete",node.nodeId)


    senderData, senderCloser, err := node.state.Get(accountKey(sender))
    if err != nil {
        if err == pebble.ErrNotFound {
            return false, fmt.Errorf("sender '%s' not found", sender)
        }
        return false, fmt.Errorf("failed to read sender: %v", err)
    }

    // log.Printf("[Node %d] Sender check complete",node.nodeId)

    senderBalance, err := deserializeBalance(senderData)
    senderCloser.Close()
    if err != nil {
        return false, fmt.Errorf("failed to deserialize sender balance: %v", err)
    }

    // log.Printf("[Node %d] Sender balance check complete",node.nodeId)

    receiverData, receiverCloser, err := node.state.Get(accountKey(receiver))
    if err != nil {
        if err == pebble.ErrNotFound {
            return false, fmt.Errorf("receiver '%s' not found", receiver)
        }
        return false, fmt.Errorf("failed to read receiver: %v", err)
    }

    // log.Printf("[Node %d] Receiver check complete",node.nodeId)

    receiverBalance, err := deserializeBalance(receiverData)
    receiverCloser.Close()
    if err != nil {
        return false, fmt.Errorf("failed to deserialize receiver balance: %v", err)
    }

    // log.Printf("[Node %d] Receiver balance check complete",node.nodeId)

    if senderBalance < amount {
        return false, fmt.Errorf("insufficient funds for %s", sender)
    }

    // log.Printf("[Node %d] Valid funds complete",node.nodeId)
    
    newSenderBal := senderBalance - amount
    newReceiverBal := receiverBalance + amount

    batch := node.state.NewBatch()
    defer batch.Close()

    // log.Printf("[Node %d] Serialize start",node.nodeId)
    sData, _ := serializeBalance(newSenderBal)
    rData, _ := serializeBalance(newReceiverBal)

    // log.Printf("[Node %d] PRE SET sender",node.nodeId)
    if err := batch.Set(accountKey(sender), sData, nil); err != nil {
        return false, err
    }
    // log.Printf("[Node %d] PRE SET receiver",node.nodeId)
    if err := batch.Set(accountKey(receiver), rData, nil); err != nil {
        return false, err
    }
    // log.Printf("[Node %d] SET complete",node.nodeId)
    if err := batch.Commit(pebble.NoSync); err != nil {
        return false, err
    }
    // log.Printf("[Node %d] COMMIT complete",node.nodeId)

    return true, nil
}

func(node *Node) processCrossShardSenderPart(sender string, amount int32) (bool, error) {
    senderData, senderCloser, err := node.state.Get(accountKey(sender))
    if err != nil {
        if err == pebble.ErrNotFound {
            return false, fmt.Errorf("sender '%s' not found", sender)
        }
        return false, fmt.Errorf("failed to read sender: %v", err)
    }

    senderBalance, err := deserializeBalance(senderData)
    senderCloser.Close()
    if err != nil {
        return false, fmt.Errorf("failed to deserialize sender balance: %v", err)
    }

    newSenderBal := senderBalance - amount

    batch := node.state.NewBatch()
    defer batch.Close()

    // log.Printf("[Node %d] Serialize start",node.nodeId)
    sData, _ := serializeBalance(newSenderBal)

    if err := batch.Set(accountKey(sender), sData, nil); err != nil {
        return false, err
    }
    if err := batch.Commit(pebble.NoSync); err != nil {
        return false, err
    }

    return true, nil
}

func(node *Node) processCrossShardReceiverPart(receiver string, amount int32) (bool, error) {
    receiverData, receiverCloser, err := node.state.Get(accountKey(receiver))
    if err != nil {
        if err == pebble.ErrNotFound {
            return false, fmt.Errorf("receiver '%s' not found", receiver)
        }
        return false, fmt.Errorf("failed to read receiver: %v", err)
    }

    receiverBalance, err := deserializeBalance(receiverData)
    receiverCloser.Close()

    if err != nil {
        return false, fmt.Errorf("failed to deserialize receiver balance: %v", err)
    }

    newReceiverBal := receiverBalance + amount

    batch := node.state.NewBatch()
    defer batch.Close()

    rData, _ := serializeBalance(newReceiverBal)

    if err := batch.Set(accountKey(receiver), rData, nil); err != nil {
        return false, err
    }
    // log.Printf("[Node %d] SET complete",node.nodeId)
    if err := batch.Commit(pebble.NoSync); err != nil {
        return false, err
    }
    // log.Printf("[Node %d] COMMIT complete",node.nodeId)

    return true, nil
}


func loadInitialState(clusterId int32,shardSize int32) BankAccounts {
    start := 1 + (clusterId-1) *shardSize
    end := clusterId *shardSize

    accounts := make([]BankAccount, 0, end-start+1)
    for id := start; id <= end; id++ {
        accounts = append(accounts, BankAccount{
            Name:    strconv.FormatInt(int64(id), 10),
            Balance: 10,
        })
    }

    return BankAccounts{Accounts: accounts}
}


func (node *Node) loadState(clusterId int32, shardSize int32) error {
    dbPath := fmt.Sprintf("state/node%d", node.nodeId)

    if err := os.MkdirAll("state", 0755); err != nil {
        return fmt.Errorf("failed to create state directory: %v", err)
    }

    newDB, err := pebble.Open(dbPath, &pebble.Options{})
    if err != nil {
        return fmt.Errorf("failed to open pebble db: %v", err)
    }

    node.muState.Lock()
    node.state = newDB
    node.muState.Unlock()


    log.Printf("[Node %d] Initializing new database with default accounts", node.nodeId)
    initialData := loadInitialState(clusterId,shardSize)
    
    batch := newDB.NewBatch()
    defer batch.Close() 


    for _, account := range initialData.Accounts {
        data, err := serializeBalance(account.Balance)
        if err != nil {
            return fmt.Errorf("failed to serialize balance for %s: %v", account.Name, err)
        }
        
        if err := batch.Set(accountKey(account.Name), data, nil); err != nil {
            return fmt.Errorf("failed to write initial account %s: %v", account.Name, err)
        }
    }
    
    if err := batch.Commit(pebble.NoSync); err != nil {
        return fmt.Errorf("failed to commit initial state: %v", err)
    }
    
    log.Printf("[Node %d] Successfully initialized %d accounts in DB", node.nodeId, len(initialData.Accounts))
    
    return nil
}


func (node *Node) DeleteStateFile() error {
    dbPath := fmt.Sprintf("state/node%d", node.nodeId)

    node.muState.Lock()
    if node.state != nil {
        if err := node.state.Close(); err != nil {
            log.Printf("[Node %d] Warning: error closing DB: %v", node.nodeId, err)
        }
        node.state = nil
    }
    node.muState.Unlock()


    if err := os.RemoveAll(dbPath); err != nil {
        if os.IsNotExist(err) {
            log.Printf("[Node %d] DB file already absent: %s", node.nodeId, dbPath)
            return nil
        }
        return fmt.Errorf("[Node %d] failed to delete DB file %s: %v", node.nodeId, dbPath, err)
    }

    log.Printf("[Node %d] Deleted DB file: %s", node.nodeId, dbPath)
    return nil
}

func accountKey(name string) []byte {
    return []byte("account:" + name)
}

func serializeBalance(balance int32) ([]byte, error) {
    buf := make([]byte, 4)
    binary.LittleEndian.PutUint32(buf, uint32(balance))
    return buf, nil
}

func deserializeBalance(data []byte) (int32, error) {
    if len(data) < 4 {
        return 0, fmt.Errorf("invalid data length")
    }
    return int32(binary.LittleEndian.Uint32(data)), nil
}

//  This can be called regardless of mutex since it is protected by our locking mechanism
// But need to be sure because muState is hold over in logic
func(node *Node) checkIfSufficientBalance(sender string, amount int32) (bool){
    senderData, senderCloser, err := node.state.Get(accountKey(sender))

    if err != nil {
        if err == pebble.ErrNotFound {
            log.Printf("[Node %d] Pebble data error 1",node.nodeId)
            return false
        }
        log.Printf("[Node %d] Pebble data error 2",node.nodeId)
        return false
    }

    senderBalance, err := deserializeBalance(senderData)
    senderCloser.Close()
    if err != nil {
        log.Printf("[Node %d] Pebble data error 3",node.nodeId)
        return false
    }

    if senderBalance < amount {
        log.Printf("[Node %d] Sender balance=%d less than transfer amount%d",node.nodeId,senderBalance,amount)
        return false
    }

    return true
}

// Helper function
func (node *Node) getBalance(accountName string) (int32, error) {
    // node.muState.RLock()
    // defer node.muState.RUnlock()
    
    data, closer, err := node.state.Get(accountKey(accountName))
    if err != nil {
        if err == pebble.ErrNotFound {
            return 0, fmt.Errorf("account '%s' not found", accountName)
        }
        return 0, fmt.Errorf("failed to read account: %v", err)
    }
    defer closer.Close()
    
    balance, err := deserializeBalance(data)
    if err != nil {
        return 0, fmt.Errorf("invalid balance data: %v", err)
    }
    
    return balance, nil
}


func (node *Node) undoSenderDebit(sender string, amount int32) (bool, error) {
    senderData, senderCloser, err := node.state.Get(accountKey(sender))
    if err != nil {
        if err == pebble.ErrNotFound {
            return false, fmt.Errorf("sender '%s' not found during undo", sender)
        }
        return false, fmt.Errorf("failed to read sender during undo: %v", err)
    }

    senderBalance, err := deserializeBalance(senderData)
    senderCloser.Close()
    if err != nil {
        return false, fmt.Errorf("failed to deserialize sender balance during undo: %v", err)
    }

    newSenderBal := senderBalance + amount

    batch := node.state.NewBatch()
    defer batch.Close()

    sData, _ := serializeBalance(newSenderBal)
    if err := batch.Set(accountKey(sender), sData, nil); err != nil {
        return false, err
    }
    if err := batch.Commit(pebble.NoSync); err != nil {
        return false, err
    }

    return true, nil
}

func (node *Node) undoReceiverCredit(receiver string, amount int32) (bool, error) {
    receiverData, receiverCloser, err := node.state.Get(accountKey(receiver))
    if err != nil {
        if err == pebble.ErrNotFound {
            return false, fmt.Errorf("receiver '%s' not found during undo", receiver)
        }
        return false, fmt.Errorf("failed to read receiver during undo: %v", err)
    }

    receiverBalance, err := deserializeBalance(receiverData)
    receiverCloser.Close()
    if err != nil {
        return false, fmt.Errorf("failed to deserialize receiver balance during undo: %v", err)
    }

    newReceiverBal := receiverBalance - amount

    batch := node.state.NewBatch()
    defer batch.Close()

    rData, _ := serializeBalance(newReceiverBal)
    if err := batch.Set(accountKey(receiver), rData, nil); err != nil {
        return false, err
    }
    if err := batch.Commit(pebble.NoSync); err != nil {
        return false, err
    }

    return true, nil
}