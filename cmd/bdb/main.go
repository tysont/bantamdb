// ABOUTME: CLI entrypoint for BantamDB. Wires together the database layers
// ABOUTME: (log, storage, coordinator) and starts the HTTP server.
package main

import (
	"bdb"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"
)

func main() {
	var port int
	var epoch time.Duration

	root := &cobra.Command{
		Use:   "bdb",
		Short: "BantamDB - a Calvin-protocol database",
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(port, epoch)
		},
	}
	root.Flags().IntVar(&port, "port", 8080, "HTTP listen port")
	root.Flags().DurationVar(&epoch, "epoch", 10*time.Millisecond, "epoch flush interval")

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(port int, epoch time.Duration) error {
	cfg := bdb.Config{
		EpochDuration: epoch,
		Port:          port,
	}

	txnLog := bdb.NewMemoryLog(cfg)
	storage := bdb.NewMemoryStorage()
	storage.Start(txnLog.Subscribe())
	coord := bdb.NewCoordinator(txnLog, storage)
	handler := bdb.NewHandler(coord)

	txnLog.Start()

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: handler,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go func() {
		log.Printf("bantamdb listening on :%d (epoch=%s)", port, epoch)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(shutdownCtx)
	txnLog.Stop()
	storage.Stop()

	return nil
}
