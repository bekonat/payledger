// Command loadtest creates accounts and sends random concurrent transfers to
// a running ledger, then prints throughput and latency percentiles.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

func main() {
	addr := flag.String("addr", "http://localhost:8080", "ledger base URL")
	accounts := flag.Int("accounts", 100, "number of accounts")
	workers := flag.Int("c", 32, "concurrent clients")
	duration := flag.Duration("d", 15*time.Second, "test duration")
	flag.Parse()

	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: *workers}}
	ids, err := setup(client, *addr, *accounts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var mu sync.Mutex
	var latencies []time.Duration
	codes := map[int]int{}
	var wg sync.WaitGroup
	start := time.Now()
	for w := range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), uint64(time.Now().UnixNano())))
			var local []time.Duration
			localCodes := map[int]int{}
			for ctx.Err() == nil {
				from, to := ids[r.IntN(len(ids))], ids[r.IntN(len(ids))]
				if from == to {
					continue
				}
				body := fmt.Sprintf(`{"from_account_id":%q,"to_account_id":%q,"amount":%d}`, from, to, r.IntN(500)+1)
				t0 := time.Now()
				code, err := post(client, *addr+"/api/v1/transfers", body, uuid.NewString())
				if err != nil {
					localCodes[0]++
					continue
				}
				local = append(local, time.Since(t0))
				localCodes[code]++
			}
			mu.Lock()
			latencies = append(latencies, local...)
			for k, v := range localCodes {
				codes[k] += v
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	slices.Sort(latencies)
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(float64(len(latencies)-1)*p)].Round(100 * time.Microsecond)
	}
	fmt.Printf("requests:   %d in %s\n", len(latencies), elapsed.Round(time.Millisecond))
	fmt.Printf("throughput: %.0f req/s\n", float64(len(latencies))/elapsed.Seconds())
	fmt.Printf("latency:    p50 %s  p95 %s  p99 %s\n", pct(.50), pct(.95), pct(.99))
	fmt.Printf("status:     %v\n", codes)
}

func setup(client *http.Client, addr string, n int) ([]string, error) {
	ids := make([]string, n)
	for i := range ids {
		resp, err := client.Post(addr+"/api/v1/accounts", "application/json",
			bytes.NewBufferString(fmt.Sprintf(`{"owner_id":"load-%d","currency":"KZT"}`, i)))
		if err != nil {
			return nil, err
		}
		var a struct {
			ID string `json:"id"`
		}
		err = json.NewDecoder(resp.Body).Decode(&a)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		ids[i] = a.ID
		body := fmt.Sprintf(`{"account_id":%q,"amount":1000000}`, a.ID)
		if _, err := post(client, addr+"/api/v1/deposits", body, uuid.NewString()); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

func post(client *http.Client, url, body, key string) (int, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}
