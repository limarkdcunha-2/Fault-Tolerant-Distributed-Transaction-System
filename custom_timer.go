/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"log"
	"sync"
	"time"
)

type CustomTimer struct {
	predefinedTime time.Duration
	stopChan       chan bool
	running        bool
	mu             sync.Mutex
	onExpired      func()
}

func NewCustomTimer(t time.Duration, onExpired func()) *CustomTimer {
	return &CustomTimer{
		predefinedTime: t,
		stopChan:       make(chan bool),
		running:        false,
		onExpired:      onExpired,
	}
}

// GetPredefinedTime returns the time given to timer at input level
func (ct *CustomTimer) GetPredefinedTime() time.Duration {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return ct.predefinedTime
}

// UpdatePredefinedTime updates the predefined time
func (ct *CustomTimer) UpdatePredefinedTime(newTime time.Duration) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.predefinedTime = newTime
}

// Start starts the timer from 0 and waits for the predefined time T
func (ct *CustomTimer) Start() {
	ct.mu.Lock()
	if ct.running {
		ct.mu.Unlock()
		return
	}
	ct.running = true
	ct.stopChan = make(chan bool)
	ct.mu.Unlock()

	go func() {
		timer := time.NewTimer(ct.predefinedTime)
		defer timer.Stop()

		select {
		case <-timer.C:
			// Timer expired naturally
			ct.mu.Lock()
			wasRunning := ct.running
			ct.running = false
			ct.mu.Unlock()
			
			if wasRunning && ct.onExpired != nil {
				log.Println("Timer expired, running callback...")
				ct.onExpired()
			}
		case <-ct.stopChan:
			// Timer was stopped/paused
			ct.mu.Lock()
			ct.running = false
			ct.mu.Unlock()
		}
	}()
}

// Stop pauses the timer
func (ct *CustomTimer) Stop() {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	if ct.running {
		close(ct.stopChan)
		ct.running = false
	}
}

func (ct *CustomTimer) Restart() {
	ct.Stop()
	// Small delay to ensure clean stop
	// DO NOT reduce this futher
	time.Sleep(100 * time.Microsecond) 
	ct.Start()
}

func (ct *CustomTimer) IsRunning() bool {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return ct.running
}