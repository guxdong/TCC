package tcc

import "time"

type Options struct {
	Timeout         time.Duration // 事务执行时长限制
	MonitorInterval time.Duration // 轮询监控任务间隔
}

type Option func(*Options)

func WithTimeout(timeout time.Duration) Option {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	return func(o *Options) {
		o.Timeout = timeout
	}
}

func WithMonitorInterval(tick time.Duration) Option {
	if tick <= 0 {
		tick = 10 * time.Second
	}

	return func(o *Options) {
		o.MonitorInterval = tick
	}
}

func repair(o *Options) {
	if o.MonitorInterval <= 0 {
		o.MonitorInterval = 10 * time.Second
	}

	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
}
