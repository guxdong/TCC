package tcc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// TxStatus 事务状态
type TxStatus int

const (
	Trying TxStatus = iota
	TryFailed
	TrySuccess
	Confirmed
	Canceled
)

type TCCManager struct {
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	Components map[string]TCCComponent
	opts       *Options
	txLog      TxLog
}

func NewTCCManager(txLog TxLog, opts ...Option) *TCCManager {
	ctx, cancel := context.WithCancel(context.Background())
	txManager := &TCCManager{
		ctx:        ctx,
		mu:         sync.Mutex{},
		Components: make(map[string]TCCComponent),
		cancel:     cancel,
		opts:       &Options{},
		txLog:      txLog,
	}
	for _, opt := range opts {
		opt(txManager.opts)
	}
	repair(txManager.opts)
	go txManager.run()
	return txManager
}

func (t *TCCManager) Stop() {
	t.cancel()
}

// Register register a component to the manager
func (t *TCCManager) Register(component TCCComponent) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	id := component.ID()
	if _, isExist := t.Components[id]; isExist {
		return errors.New("component already registered")
	}

	t.Components[id] = component
	return nil
}

// getComponents get components by id
func (t *TCCManager) getComponents(ids []string) ([]TCCComponent, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	components := make([]TCCComponent, 0, len(ids))
	for _, id := range ids {
		component, isExist := t.Components[id]
		if !isExist {
			return nil, errors.New("component not found")
		}
		components = append(components, component)
	}

	return components, nil
}

func (t *TCCManager) Transaction(ctx context.Context, componentIDs []string) (txID string, success bool, err error) {
	transCtx, cancel := context.WithTimeout(ctx, t.opts.Timeout)
	defer cancel()

	components, err := t.getComponents(componentIDs)
	if err != nil {
		return "", false, err
	}
	// 全局唯一的事务ID
	txID = ulid.Make().String()

	// 先记录事务日志，标记事务处于processing状态，记录创建时间，再执行try。
	// 如果记录事务日志失败，没有任何影响。
	// 如果记录事务日志成功之后Manager宕机，重启后异步轮询时会发现事务处于processing状态且已超时，便会执行cancel。
	err = t.txLog.CreateTxLog(transCtx, txID, componentIDs)
	if err != nil {
		fmt.Println("failed to CreateTxLog when Transaction")
		return "", false, err
	}

	errChan := make(chan error, len(components))
	go func() {
		var wg sync.WaitGroup
		for i := range components {
			component := components[i]
			wg.Add(1)
			// 并发执行try操作
			go func(component TCCComponent) {
				defer wg.Done()
				response, err := component.Try(transCtx, &TryRequest{TransactionID: txID})
				// 有一个component的try操作失败，则事务执行失败
				if err != nil || !response.ACK {
					fmt.Printf("component '%s' try failed, txID: %s\n", component.ID(), txID)
					errChan <- fmt.Errorf("component '%s' try failed", component.ID())
					if err := t.txLog.UpdateTxStatus(transCtx, txID, TryFailed); err != nil {
						fmt.Printf("update tx log to try failed failed, txID: %s, err: %v\n", txID, err)
						errChan <- fmt.Errorf("update tx log to try failed failed")
					}
					return
				}

			}(component)
		}
		wg.Wait()
		close(errChan)
	}()

	success = true
	// 有一个component的try操作失败，则事务执行失败
	if err := <-errChan; err != nil {
		cancel()
		success = false
	}
	// 如果所有component的try操作都成功，则更新事务日志为try success
	if success {
		if err := t.txLog.UpdateTxStatus(ctx, txID, TrySuccess); err != nil {
			fmt.Printf("update tx log to try success failed, txID: %s, err: %v\n", txID, err)
			success = false
		}
	}
	// 执行confirm或cancel
	t.confirmOrCancel(ctx, txID, components)
	// 根据try的结果
	return txID, success, nil
}

func (t *TCCManager) confirmOrCancel(ctx context.Context, txID string, components []TCCComponent) error {
	record, err := t.txLog.GetTxLog(ctx, txID)
	if err != nil {
		fmt.Println("failed to GetTxLog when confirmOrCancel")
		return err
	}

	if record.Status == Confirmed || record.Status == Canceled {
		return nil
	}

	var secStageFunc func(ctx context.Context, component TCCComponent) (*TCCResponse, error)
	var status TxStatus
	if record.Status == TrySuccess {
		secStageFunc = func(ctx context.Context, component TCCComponent) (*TCCResponse, error) {
			// 对 component 进行第二阶段的 confirm 操作
			return component.Confirm(ctx, txID)
		}
		status = Confirmed
	} else if record.Status == TryFailed {
		secStageFunc = func(ctx context.Context, component TCCComponent) (*TCCResponse, error) {
			// 对 component 进行第二阶段的 cancel 操作
			return component.Cancel(ctx, txID)
		}
		status = Canceled
	} else {
		// 如果事务状态是处理中，并且已超时，则认为执行try失败
		if record.CreatedAt.Before(time.Now().Add(-t.opts.Timeout)) {
			secStageFunc = func(ctx context.Context, component TCCComponent) (*TCCResponse, error) {
				// 对 component 进行第二阶段的 cancel 操作
				return component.Cancel(ctx, txID)
			}
			status = Canceled
		} else {
			return nil
		}
	}

	for _, component := range components {
		resp, err := secStageFunc(ctx, component)
		if err != nil {
			fmt.Printf("component '%s' confirm or cancel failed, txID: %s, err: %v\n", component.ID(), txID, err)
			return err
		}
		if !resp.ACK {
			fmt.Printf("component '%s' confirm or cancel failed, txID: %s\n", component.ID(), txID)
			return errors.New("component confirm or cancel failed")
		}
	}
	// 更新事务状态。如果更新失败也没关系，异步轮询时会再次根据事务状态执行confirm或cancel
	return t.txLog.UpdateTxStatus(ctx, txID, status)
}

// increaseInterval 增加轮询间隔，最大不超过 MonitorInterval << 3
func (t *TCCManager) increaseInterval(interval time.Duration) time.Duration {
	interval <<= 1
	if threshold := t.opts.MonitorInterval << 3; interval > threshold {
		interval = threshold
	}
	return interval
}

// batchProcessTx 批量处理事务
func (t *TCCManager) batchProcessTx(records []TxRecord) error {
	errCh := make(chan error)
	go func() {
		var wg sync.WaitGroup
		for i := range records {
			record := records[i]
			components, err := t.getComponents(record.ComponentIDs)
			if err != nil {
				fmt.Println("failed to getComponents when batchProcessTx")
				continue
			}
			wg.Add(1)
			go func(txID string, components []TCCComponent) {
				defer wg.Done()
				if err := t.confirmOrCancel(t.ctx, txID, components); err != nil {
					errCh <- err
				}
			}(record.TxID, components)
		}
		wg.Wait()
		close(errCh)
	}()

	var firstErr error
	// 通过 chan 阻塞在这里，直到所有 goroutine 执行完成，chan 被 close 才能继续
	for err := range errCh {
		// 记录遇到的第一个错误
		if firstErr != nil {
			continue
		}
		firstErr = err
	}

	return firstErr
}

func (t *TCCManager) run() {
	var interval time.Duration
	var err error
	for {
		if err == nil {
			interval = t.opts.MonitorInterval
		} else {
			interval = t.increaseInterval(interval)
		}
		select {
		case <-t.ctx.Done():
			return
		case <-time.After(interval):
			// 获取未完成的事务
			records, err := t.txLog.GetUnfinishedTx(t.ctx, t.opts.Timeout)
			if err != nil {
				fmt.Println("failed to GetUnfinishedTx when execute run")
				return
			}
			err = t.batchProcessTx(records)
		}
	}
}
