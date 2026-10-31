package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// GCRA (generic cell rate algorithm) stores a single number per key: the
// theoretical arrival time (TAT) of the next request. It behaves like a token
// bucket with capacity burst, but needs one key instead of two and is
// updated atomically by one Lua script.
var gcraScript = redis.NewScript(`
local now       = tonumber(ARGV[1])
local emission  = tonumber(ARGV[2])
local tolerance = tonumber(ARGV[3])
local tat = tonumber(redis.call('GET', KEYS[1]) or now)
if tat < now then
  tat = now
end
local new_tat = tat + emission
local allow_at = new_tat - tolerance
if allow_at > now then
  return {0, allow_at - now}
end
-- values are microseconds, PX takes milliseconds
redis.call('SET', KEYS[1], new_tat, 'PX', math.ceil((new_tat - now) / 1000))
return {1, 0}
`)

type Limiter struct {
	rdb       redis.Scripter
	emission  time.Duration
	tolerance time.Duration
	prefix    string
	now       func() time.Time
}

// New allows rate requests per second on average with bursts of up to burst
// requests.
func New(rdb redis.Scripter, rate float64, burst int) *Limiter {
	emission := time.Duration(float64(time.Second) / rate)
	return &Limiter{
		rdb:       rdb,
		emission:  emission,
		tolerance: emission * time.Duration(burst),
		prefix:    "ratelimit:",
		now:       time.Now,
	}
}

func (l *Limiter) Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration, err error) {
	now := l.now().UnixMicro()
	res, err := gcraScript.Run(ctx, l.rdb, []string{l.prefix + key},
		now, l.emission.Microseconds(), l.tolerance.Microseconds()).Int64Slice()
	if err != nil {
		return false, 0, err
	}
	return res[0] == 1, time.Duration(res[1]) * time.Microsecond, nil
}
