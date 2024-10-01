package tcc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"
)

type TxRecord struct {
	TxID         string    // 事务ID
	ComponentIDs []string  // 参与事务的组件ID列表
	Status       TxStatus  // 事务状态
	CreatedAt    time.Time // 事务创建时间
	UpdatedAt    time.Time // 事务更新时间
}

// TxLog is the interface for transaction log
type TxLog interface {
	// CreateTxLog 创建一条事务日志
	CreateTxLog(ctx context.Context, txID string, componentIDs []string) error
	// UpdateTxStatus 更新事务状态
	UpdateTxStatus(ctx context.Context, txID string, status TxStatus) error
	// GetTxLog 根据事务ID获取事务日志
	GetTxLog(ctx context.Context, txID string) (TxRecord, error)
	// GetUnfinishedTx 获取未完成的事务ID，包括：try超时的事务、try执行完成但第二阶段操作未完成或需要重试的事务
	GetUnfinishedTx(ctx context.Context, timeout time.Duration) ([]TxRecord, error)
}

var (
	txLogOnce sync.Once
	t         *txLog
)

type txLog struct {
	db               *sql.DB
	statusMap        map[TxStatus]int
	statusMapReverse map[int]TxStatus
}

func NewTxLog(db *sql.DB) *txLog {
	txLogOnce.Do(func() {
		t = &txLog{
			db: db,
			statusMap: map[TxStatus]int{
				Trying:     1,
				TryFailed:  2,
				TrySuccess: 3,
				Confirmed:  4,
				Canceled:   5,
			},
			statusMapReverse: map[int]TxStatus{
				1: Trying,
				2: TryFailed,
				3: TrySuccess,
				4: Confirmed,
				5: Canceled,
			},
		}
	})
	return t
}

// CreateLog creates a new log entry for a transaction
func (t *txLog) CreateTxLog(ctx context.Context, txID string, componentIDs []string) error {
	componentIDsStr := strings.Join(componentIDs, ",")
	_, err := t.db.ExecContext(ctx, "INSERT INTO tx_log (tx_id, component_ids, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)", txID, componentIDsStr, 1, time.Now().UTC(), time.Now().UTC())
	if err != nil {
		fmt.Println("failed to create tx log, err:", err)
	}
	return err
}

// UpdateTxStatus updates the status of a transaction log entry
func (t *txLog) UpdateTxStatus(ctx context.Context, txID string, status TxStatus) error {
	_, err := t.db.ExecContext(ctx, "UPDATE tx_log SET status = ?, updated_at = ? WHERE tx_id = ?", t.statusMap[status], time.Now().UTC(), txID)
	if err != nil {
		fmt.Println("failed to update tx status, err:", err)
	}
	return err
}

// GetTxLog gets the transaction record
func (t *txLog) GetTxLog(ctx context.Context, txID string) (TxRecord, error) {
	var componentIDsStr string
	var status int
	var createdAt time.Time
	var updatedAt time.Time
	err := t.db.QueryRowContext(ctx, "SELECT component_ids, status, created_at, updated_at FROM tx_log WHERE tx_id = ?", txID).Scan(&componentIDsStr, &status, &createdAt, &updatedAt)
	if err != nil {
		fmt.Println("failed to execute query when GetTxLog, err:", err)
		return TxRecord{}, err
	}
	componentIDs := strings.Split(componentIDsStr, ",")
	record := TxRecord{
		TxID:         txID,
		ComponentIDs: componentIDs,
		Status:       t.statusMapReverse[status],
		CreatedAt:    createdAt,
		UpdatedAt:    updatedAt,
	}
	return record, nil
}

// GetUnfinishedTx 获取未完成的事务ID，包括：try超时的事务、try执行完成但第二阶段操作未完成或需要重试的事务
func (t *txLog) GetUnfinishedTx(ctx context.Context, timeout time.Duration) ([]TxRecord, error) {
	rows, err := t.db.QueryContext(ctx, "SELECT tx_id, component_ids, status FROM tx_log WHERE (status = 1 AND created_at < ?) OR status = 2 OR status = 3", time.Now().Add(-timeout))
	if err != nil {
		return nil, err
	}
	var records []TxRecord
	for rows.Next() {
		var record TxRecord
		var components string
		var status int
		err = rows.Scan(&record.TxID, &components, &status)
		if err != nil {
			fmt.Println("failed to scan record when GetUnfinishedTx, err:", err)
			return nil, err
		}
		record.ComponentIDs = strings.Split(components, ",")
		record.Status = t.statusMapReverse[status]
		records = append(records, record)
	}
	return records, nil
}
