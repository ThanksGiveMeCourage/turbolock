package turbolock

import "time"

// 使用 Functional Options（函数式选项）模式

const (
	defaultOptExpiry     = 8 * time.Second       // 锁默认TTL
	defaultOptTries      = 32                    // 默认重试次数
	defaultOptRetryDelay = 50 * time.Millisecond // 默认重试延迟
)

// Options 包含分布式锁的所有配置项
type Options struct {
	Expiry          time.Duration // 锁的过期时间（TTL）
	Tries           int           // 抢锁的最大重试次数
	RetryDelay      time.Duration // 每次重试的固定延迟
	AutoRenew       bool          // 是否开启自动续期
	MaxHoldDuration time.Duration // 单次持锁的最长时间（0=不限）
}

// Option 定义函数式选项的函数签名
type Option func(*Options)

// defaultOptions 提供一套安全的默认值
func defaultOptions() *Options {
	return &Options{
		Expiry:          defaultOptExpiry,
		Tries:           defaultOptTries,
		RetryDelay:      defaultOptRetryDelay,
		AutoRenew:       false,            // 默认关闭，保持向后兼容
		MaxHoldDuration: 30 * time.Second, // 默认最多持锁30s
	}
}

// WithExpiry 设置锁的过期时间
func WithExpiry(expiry time.Duration) Option {
	return func(o *Options) {
		o.Expiry = expiry
	}
}

// WithTries 设置重试次数
func WithTries(tries int) Option {
	return func(o *Options) {
		o.Tries = tries
	}
}

// WithRetryDelay 设置重试间隔
func WithRetryDelay(delay time.Duration) Option {
	return func(o *Options) {
		o.RetryDelay = delay
	}
}

// WithAutoRenew 开启自动续期（renewInterval 为续期间隔，0 表示使用 Expiry/3）
func WithAutoRenew() Option {
	return func(o *Options) {
		o.AutoRenew = true
	}
}

// WithMaxHoldDuration 设置单次最长持锁时间
func WithMaxHoldDuration(d time.Duration) Option {
	return func(o *Options) {
		o.MaxHoldDuration = d
	}
}
