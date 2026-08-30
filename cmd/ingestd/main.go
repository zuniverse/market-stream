package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/zuniverse/market-stream/internal/exchange/binance"
	"github.com/zuniverse/market-stream/internal/model"
)

const (
	wsBase      = "wss://stream.binance.com:9443/ws"
	restBaseURL = "https://api.binance.com"
)

func main() {
	stream := flag.String("stream", "btcusdt@aggTrade", "Binance stream name")
	interval := flag.Duration("interval", 5*time.Second, "reporting interval")
	chanCap := flag.Int("chan-cap", 1024, "frame channel capacity")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	httpClient := &http.Client{Timeout: 10 * time.Second}
	cache, err := binance.FetchExchangeInfo(ctx, httpClient, restBaseURL)
	if err != nil {
		log.Fatal(err)
	}
	dec := binance.NewDecoder(cache)

	frames := make(chan binance.Frame, *chanCap)
	tr := binance.NewTransport(wsBase+"/"+*stream, frames)

	errc := make(chan error, 1)
	go func() { errc <- tr.Run(ctx) }()

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	var window, total int64
	for {
		select {
		case f := <-frames:
			window++
			total++
			ev, err := dec.Decode(f)
			if err != nil {
				log.Printf("decode: %v", err)
				continue
			}
			if ev.Kind == model.KindTrade {
				inst, ok := cache.LookupByNormalized(ev.Trade.Symbol)
				if !ok {
					continue
				}
				side := "sell"
				if ev.Trade.IsBuy {
					side = "buy"
				}
				fmt.Printf("trade %s price=%s qty=%s side=%s\n",
					ev.Trade.Symbol,
					formatFixed(int64(ev.Trade.Price), inst.PriceDecimals),
					formatFixed(int64(ev.Trade.Qty), inst.QtyDecimals),
					side,
				)
			}
		case <-ticker.C:
			fmt.Printf("time=%s frames=%d total=%d\n",
				time.Now().UTC().Format(time.RFC3339), window, total)
			window = 0
		case err := <-errc:
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Fatal(err)
			}
			return
		}
	}
}

// formatFixed formats a fixed-point int64 as a decimal string.
// Assumes v >= 0.
func formatFixed(v int64, decimals int) string {
	if decimals == 0 {
		return strconv.FormatInt(v, 10)
	}
	scale := int64(1)
	for i := 0; i < decimals; i++ {
		scale *= 10
	}
	return fmt.Sprintf("%d.%0*d", v/scale, decimals, v%scale)
}
