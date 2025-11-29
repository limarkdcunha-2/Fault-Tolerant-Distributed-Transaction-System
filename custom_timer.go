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

func (ct *CustomTimer) Start() {
	ct.mu.Lock()
	if ct.running {
		ct.mu.Unlock()
		return
	}
	ct.running = true
	ct.stopChan = make(chan bool)
	currentStopChan := ct.stopChan
	ct.mu.Unlock()

	go func() {
		timer := time.NewTimer(ct.predefinedTime)
		defer timer.Stop()

		select {
		case <-timer.C:
			// Timer expired naturally
			ct.mu.Lock()
			if ct.stopChan != currentStopChan {
				ct.mu.Unlock()
				return
			}

			ct.running = false
			ct.mu.Unlock()
			
			if ct.onExpired != nil {
				log.Println("[Helper] Timer expired, running callback...")
				ct.onExpired()
			}
		case <-ct.stopChan:
			// Timer was stopped/paused
			ct.mu.Lock()
			if ct.stopChan == currentStopChan {
				ct.running = false
			}
			ct.mu.Unlock()
		}
	}()
}

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
	// time.Sleep(100 * time.Microsecond) 
	ct.Start()
}

func (ct *CustomTimer) IsRunning() bool {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return ct.running
}

func (ct *CustomTimer) GetPredefinedTime() time.Duration {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return ct.predefinedTime
}

func (ct *CustomTimer) UpdatePredefinedTime(newTime time.Duration) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.predefinedTime = newTime
}