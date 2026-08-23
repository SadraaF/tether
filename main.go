// Command tether serves a resilient browser terminal over HTTP+WebSocket.
package main

import (
	"embed"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"tether/internal/server"
	"tether/internal/session"
)

//go:embed all:web/dist
var distFS embed.FS

func main() {
	port := flag.String("p", "7690", "listen port")
	addr := flag.String("a", "0.0.0.0", "bind address")
	cred := flag.String("c", "", "basic auth as user:pass (optional)")
	idle := flag.Duration("idle", 30*time.Minute, "idle timeout for sessions without viewers (0 = keep indefinitely)")
	shared := flag.Bool("shared", false, "attach every client to one shared session")
	sh := flag.String("sh", "", `custom command to spawn instead of the login shell, e.g. -sh "herdr"`)
	uploads := flag.Bool("uploads", true, "enable browser file uploads (POST /upload)")
	uploadDir := flag.String("uploaddir", filepath.Join(home(), "tether-uploads"), "directory for uploaded files")
	maxUpload := flag.Int64("maxupload", 64<<20, "per-file upload cap in bytes")
	rtcPort := flag.Int("rtcport", 8443, "TCP port for ICE-TCP candidates (datagram path over UDP-blocked networks); 0 disables")
	turnURL := flag.String("turn", "", "TURN url served to clients via /ice, e.g. turn:10.0.0.5:3478?transport=tcp; empty disables")
	turnUser := flag.String("turn-user", "", "TURN username")
	turnPass := flag.String("turn-pass", "", "TURN password")
	flag.Parse()

	cfg := session.Config{IdleTimeout: *idle, Env: []string{"TETHER_PORT=" + *port}}
	if *sh != "" {
		cfg.Command = []string{"/bin/sh", "-c", *sh}
	}
	mgr := session.NewManager(cfg)
	static, err := fs.Sub(distFS, "web/dist")
	if err != nil {
		log.Fatalf("embed: %v", err)
	}

	opts := server.Options{Mgr: mgr, Static: http.FS(static), Shared: *shared, RTCPort: *rtcPort,
		TurnURL: *turnURL, TurnUser: *turnUser, TurnPass: *turnPass}
	if *uploads {
		if err := os.MkdirAll(*uploadDir, 0o755); err != nil {
			log.Fatalf("uploaddir: %v", err)
		}
		opts.UploadDir = *uploadDir
		opts.MaxUpload = *maxUpload
	}

	if *cred != "" {
		user, pass, ok := strings.Cut(*cred, ":")
		if !ok {
			log.Fatalf("-c expects user:pass")
		}
		opts.AuthUser, opts.AuthPass = user, pass
	}

	srv := server.New(opts)

	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for range t.C {
			mgr.Reap()
		}
	}()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Printf("shutting down: killing session trees")
		mgr.KillAll()
		time.Sleep(300 * time.Millisecond) // let readers observe EIO
		os.Exit(0)
	}()

	log.Printf("tether listening on %s:%s", *addr, *port)
	if err := http.ListenAndServe(*addr+":"+*port, srv.Handler()); err != nil {
		log.Fatal(err)
	}
}

// home resolves the current user's home directory for the default upload dir.
func home() string {
	h, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	return h
}
