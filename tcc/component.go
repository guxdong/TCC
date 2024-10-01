package tcc

import "context"

// TCCComponent is a component that implements the TCC pattern
type TCCComponent interface {
	// ID 组件唯一标识
	ID() string
	// Try 执行try操作
	Try(ctx context.Context, request *TryRequest) (*TCCResponse, error)
	// Confirm 执行confirm操作
	Confirm(ctx context.Context, txID string) (*TCCResponse, error)
	// Cancel 执行cancel操作
	Cancel(ctx context.Context, txID string) (*TCCResponse, error)
}

type TryRequest struct {
	TransactionID string // 全局事务ID
}

type TCCResponse struct {
	ComponentID   string // 组件ID
	ACK           bool
	TransactionID string // 全局事务ID
}
