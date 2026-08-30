package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/zuniverse/market-stream/internal/exchange/binance"
)

const wsBase = "wss://stream.binance.com:9443/ws"

func main() {
	stream := flag.String("stream", "btcusdt@aggTrade", "Binance stream name")
	interval := flag.Duration("interval", 5*time.Second, "reporting interval")
	chanCap := flag.Int("chan-cap", 1024, "frame channel capacity")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	frames := make(chan binance.Frame, *chanCap)
	conn := binance.NewConn(wsBase+"/"+*stream, frames)

	errc := make(chan error, 1)
	go func() { errc <- conn.Run(ctx) }()

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	var window, total int64
	for {
		select {
		case <-frames:
			window++
			total++
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
