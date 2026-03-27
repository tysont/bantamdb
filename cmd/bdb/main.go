// ABOUTME: CLI entrypoint for BantamDB. Wires together the database layers
// ABOUTME: and starts the server, supporting both single-node and clustered modes.
package main

import (
	"bdb"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func main() {
	var cfg bdb.Config
	var peers string

	root := &cobra.Command{
		Use:   "bdb",
		Short: "BantamDB - a Calvin-protocol distributed database",
		RunE: func(cmd *cobra.Command, args []string) error {
			if peers != "" {
				cfg.Peers = strings.Split(peers, ",")
			}
			return run(cfg)
		},
	}
	root.Flags().IntVar(&cfg.Port, "port", 8080, "HTTP listen port")
	root.Flags().DurationVar(&cfg.EpochDuration, "epoch", 10*time.Millisecond, "epoch flush interval")
	root.Flags().StringVar(&cfg.NodeID, "node-id", "", "unique node identifier")
	root.Flags().StringVar(&cfg.RaftAddr, "raft-addr", "", "Raft bind address (enables clustered mode)")
	root.Flags().StringVar(&peers, "peers", "", "comma-separated peer addresses")
	root.Flags().BoolVar(&cfg.Bootstrap, "bootstrap", false, "bootstrap a new cluster")

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cfg bdb.Config) error {
	mux := http.NewServeMux()

	var txnLog bdb.Log
	if cfg.RaftAddr != "" {
		// Clustered mode with Raft
		if cfg.NodeID == "" {
			cfg.NodeID = bdb.RandomString(8)
		}
		transport := bdb.NewHTTPTransport()
		rl := bdb.NewRaftLog(cfg, cfg.NodeID, cfg.Peers, transport)
		bdb.RegisterRaftHandlers(mux, rl.Node())
		bdb.RegisterClusterHandlers(mux, rl)
		txnLog = rl
	} else {
		// Single-node mode with in-memory log
		txnLog = bdb.NewMemoryLog(cfg)
	}

	storage := bdb.NewMemoryStorage()
	storage.Start(txnLog.Subscribe())
	coord := bdb.NewCoordinator(txnLog, storage)
	handler := bdb.NewHandler(coord)
	mux.Handle("/documents", handler)
	mux.Handle("/documents/", handler)
	mux.Handle("/transactions", handler)

	txnLog.Start()

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go func() {
		mode := "single-node"
		if cfg.RaftAddr != "" {
			mode = fmt.Sprintf("clustered (node=%s, peers=%v)", cfg.NodeID, cfg.Peers)
		}
		log.Printf("bantamdb listening on :%d (%s, epoch=%s)", cfg.Port, mode, cfg.EpochDuration)
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
