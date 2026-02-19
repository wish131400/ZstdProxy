package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
)

var proxyV2Signature = []byte{
	0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a,
}

type config struct {
	Mode string

	Listen string
	Target string

	MaxConnectionsPerIP int
	MaxRequests         int
	WindowDuration      time.Duration
	BanDuration         time.Duration

	Level int

	FlushInterval     time.Duration
	MaxRatePerConnBps int64
	MaxRateGlobalBps  int64
	BurstBytes        int64
}

type proxyInfo struct {
	Valid      bool
	SourceIP   string
	SourcePort int
	TargetIP   string
	TargetPort int
}

type guardEntry struct {
	Active      int
	Requests    []time.Time
	BannedUntil time.Time
}

type floodGuard struct {
	mu    sync.Mutex
	state map[string]*guardEntry
	cfg   config
}

func newFloodGuard(cfg config) *floodGuard {
	return &floodGuard{
		state: make(map[string]*guardEntry),
		cfg:   cfg,
	}
}

func (g *floodGuard) begin(ip string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	e := g.state[ip]
	if e == nil {
		e = &guardEntry{}
		g.state[ip] = e
	}

	if e.BannedUntil.After(now) {
		return fmt.Errorf("ip %s is banned until %s", ip, e.BannedUntil.Format(time.RFC3339))
	}

	if g.cfg.MaxRequests > 0 && g.cfg.WindowDuration > 0 {
		cutoff := now.Add(-g.cfg.WindowDuration)
		kept := e.Requests[:0]
		for _, t := range e.Requests {
			if t.After(cutoff) {
				kept = append(kept, t)
			}
		}
		e.Requests = append(kept, now)
		if len(e.Requests) > g.cfg.MaxRequests {
			if g.cfg.BanDuration > 0 {
				e.BannedUntil = now.Add(g.cfg.BanDuration)
			}
			return fmt.Errorf("ip %s exceeded request rate in %s", ip, g.cfg.WindowDuration)
		}
	}

	if g.cfg.MaxConnectionsPerIP > 0 && e.Active >= g.cfg.MaxConnectionsPerIP {
		return fmt.Errorf("ip %s exceeded max concurrent connections (%d)", ip, g.cfg.MaxConnectionsPerIP)
	}

	e.Active++
	return nil
}

func (g *floodGuard) end(ip string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	e := g.state[ip]
	if e == nil {
		return
	}
	if e.Active > 0 {
		e.Active--
	}

	now := time.Now()
	if e.Active == 0 && len(e.Requests) == 0 && !e.BannedUntil.After(now) {
		delete(g.state, ip)
	}
}

type server struct {
	cfg           config
	guard         *floodGuard
	stats         *trafficStats
	globalLimiter *tokenBucket
}

type trafficStats struct {
	rawBytes   uint64
	zstdBytes  uint64
	activeConn int64
}

func (t *trafficStats) addRaw(n int) {
	if n > 0 {
		atomic.AddUint64(&t.rawBytes, uint64(n))
	}
}

func (t *trafficStats) addZstd(n int) {
	if n > 0 {
		atomic.AddUint64(&t.zstdBytes, uint64(n))
	}
}

func (t *trafficStats) addConn(delta int64) {
	atomic.AddInt64(&t.activeConn, delta)
}

func (t *trafficStats) snapshot() (raw uint64, zstd uint64, conns int64) {
	return atomic.LoadUint64(&t.rawBytes), atomic.LoadUint64(&t.zstdBytes), atomic.LoadInt64(&t.activeConn)
}

type countingReader struct {
	r  io.Reader
	cb func(int)
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 && c.cb != nil {
		c.cb(n)
	}
	return n, err
}

type countingWriter struct {
	w  io.Writer
	cb func(int)
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 && c.cb != nil {
		c.cb(n)
	}
	return n, err
}

type tokenBucket struct {
	mu       sync.Mutex
	rateBps  float64
	capacity float64
	tokens   float64
	last     time.Time
}

func newTokenBucket(rateBps, burstBytes int64) *tokenBucket {
	if rateBps <= 0 {
		return nil
	}
	if burstBytes <= 0 {
		burstBytes = rateBps
	}

	now := time.Now()
	capacity := float64(burstBytes)
	return &tokenBucket{
		rateBps:  float64(rateBps),
		capacity: capacity,
		tokens:   capacity,
		last:     now,
	}
}

func (tb *tokenBucket) waitN(n int) {
	if tb == nil || n <= 0 {
		return
	}

	remaining := float64(n)
	for remaining > 0 {
		chunk := remaining
		if chunk > tb.capacity {
			chunk = tb.capacity
		}
		tb.waitChunk(chunk)
		remaining -= chunk
	}
}

func (tb *tokenBucket) waitChunk(need float64) {
	for {
		tb.mu.Lock()

		now := time.Now()
		elapsed := now.Sub(tb.last).Seconds()
		if elapsed > 0 {
			tb.tokens += elapsed * tb.rateBps
			if tb.tokens > tb.capacity {
				tb.tokens = tb.capacity
			}
		}
		tb.last = now

		if tb.tokens >= need {
			tb.tokens -= need
			tb.mu.Unlock()
			return
		}

		shortage := need - tb.tokens
		waitSeconds := shortage / tb.rateBps
		tb.mu.Unlock()

		if waitSeconds <= 0 {
			continue
		}
		time.Sleep(time.Duration(waitSeconds * float64(time.Second)))
	}
}

type rateLimitedWriter struct {
	dst         io.Writer
	perConn     *tokenBucket
	global      *tokenBucket
	maxChunkLen int
}

func newRateLimitedWriter(dst io.Writer, perConn, global *tokenBucket) io.Writer {
	if perConn == nil && global == nil {
		return dst
	}
	return &rateLimitedWriter{
		dst:         dst,
		perConn:     perConn,
		global:      global,
		maxChunkLen: 16 * 1024,
	}
}

func (w *rateLimitedWriter) Write(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		end := written + w.maxChunkLen
		if end > len(p) {
			end = len(p)
		}

		chunk := p[written:end]
		if w.perConn != nil {
			w.perConn.waitN(len(chunk))
		}
		if w.global != nil {
			w.global.waitN(len(chunk))
		}

		n, err := w.dst.Write(chunk)
		if n > 0 {
			written += n
		}
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func main() {
	cfg := parseFlags()
	if cfg.Mode != "server" {
		log.Fatalf("only -mode server is supported, got: %s", cfg.Mode)
	}

	s := &server{
		cfg:           cfg,
		guard:         newFloodGuard(cfg),
		stats:         &trafficStats{},
		globalLimiter: newTokenBucket(cfg.MaxRateGlobalBps, cfg.BurstBytes),
	}

	if err := s.run(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func parseFlags() config {
	var cfg config

	flag.StringVar(&cfg.Mode, "mode", "server", "Run mode (server)")
	flag.StringVar(&cfg.Listen, "l", "0.0.0.0:9000", "Listen address (host:port)")
	flag.StringVar(&cfg.Target, "r", "127.0.0.1:25565", "Target MC server address (host:port)")

	flag.IntVar(&cfg.MaxConnectionsPerIP, "mc", 20, "Max concurrent connections per source IP")
	flag.IntVar(&cfg.MaxRequests, "mr", 30, "Max requests per source IP in window")
	flag.DurationVar(&cfg.WindowDuration, "wd", 10*time.Second, "Request rate window duration (e.g. 1s, 10s, 10m)")
	flag.DurationVar(&cfg.BanDuration, "bd", 30*time.Minute, "Ban duration after rate limit exceeded (e.g. 30m)")

	flag.IntVar(&cfg.Level, "level", 3, "Zstd compression level for MC->client direction")
	flag.DurationVar(&cfg.FlushInterval, "flush", 8*time.Millisecond, "Flush interval for compressed stream (e.g. 0ms, 5ms, 20ms)")
	flag.Int64Var(&cfg.MaxRatePerConnBps, "rpc", 0, "Max bytes/sec per connection on compressed output (0 disables)")
	flag.Int64Var(&cfg.MaxRateGlobalBps, "rg", 0, "Max bytes/sec globally on compressed output (0 disables)")
	flag.Int64Var(&cfg.BurstBytes, "burst", 256*1024, "Token bucket burst size in bytes for rate limits")
	flag.Parse()

	if cfg.FlushInterval < 0 {
		cfg.FlushInterval = 0
	}
	if cfg.BurstBytes <= 0 {
		cfg.BurstBytes = 256 * 1024
	}

	return cfg
}

func (s *server) run() error {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.cfg.Listen, err)
	}
	defer ln.Close()

	log.Printf("GoZstdServer started: listen=%s target=%s mode=%s", s.cfg.Listen, s.cfg.Target, s.cfg.Mode)
	log.Printf("guard: max_conn=%d max_req=%d window=%s ban=%s", s.cfg.MaxConnectionsPerIP, s.cfg.MaxRequests, s.cfg.WindowDuration, s.cfg.BanDuration)
	log.Printf("tuning: flush=%s rate_per_conn=%dB/s rate_global=%dB/s burst=%dB", s.cfg.FlushInterval, s.cfg.MaxRatePerConnBps, s.cfg.MaxRateGlobalBps, s.cfg.BurstBytes)
	stopStats := make(chan struct{})
	defer close(stopStats)
	go startStatsPrinter(stopStats, s.stats)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Printf("accept error: %v", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *server) handleConn(clientConn net.Conn) {
	defer clientConn.Close()
	s.stats.addConn(1)
	defer s.stats.addConn(-1)

	remoteIP := ipFromAddr(clientConn.RemoteAddr())
	reader := bufio.NewReader(clientConn)

	ppInfo, payloadReader, err := parseProxyProtocolV2(reader)
	if err != nil {
		log.Printf("conn %s parse proxy header failed: %v", remoteIP, err)
		return
	}

	sourceIP := remoteIP
	if ppInfo.Valid && ppInfo.SourceIP != "" {
		sourceIP = ppInfo.SourceIP
	}

	if err := s.guard.begin(sourceIP); err != nil {
		log.Printf("blocked %s: %v", sourceIP, err)
		return
	}
	defer s.guard.end(sourceIP)

	targetConn, err := net.Dial("tcp", s.cfg.Target)
	if err != nil {
		log.Printf("dial target %s failed for %s: %v", s.cfg.Target, sourceIP, err)
		return
	}
	defer targetConn.Close()

	log.Printf("accepted source=%s remote=%s target=%s", sourceIP, remoteIP, s.cfg.Target)
	perConnLimiter := newTokenBucket(s.cfg.MaxRatePerConnBps, s.cfg.BurstBytes)

	errCh := make(chan error, 2)

	go func() {
		errCh <- forwardDecompress(targetConn, payloadReader, s.stats)
		closeWrite(targetConn)
	}()

	go func() {
		errCh <- forwardCompress(clientConn, targetConn, s.cfg.Level, s.cfg.FlushInterval, s.stats, perConnLimiter, s.globalLimiter)
		closeWrite(clientConn)
	}()

	err1 := <-errCh
	err2 := <-errCh

	if isRealPipeErr(err1) {
		log.Printf("pipe error source=%s dir=client->mc: %v", sourceIP, err1)
	}
	if isRealPipeErr(err2) {
		log.Printf("pipe error source=%s dir=mc->client: %v", sourceIP, err2)
	}
}

func parseProxyProtocolV2(r io.Reader) (proxyInfo, io.Reader, error) {
	var out proxyInfo

	first := make([]byte, len(proxyV2Signature))
	if _, err := io.ReadFull(r, first); err != nil {
		return out, nil, err
	}

	if !bytes.Equal(first, proxyV2Signature) {
		return out, io.MultiReader(bytes.NewReader(first), r), nil
	}

	fixed := make([]byte, 4)
	if _, err := io.ReadFull(r, fixed); err != nil {
		return out, nil, err
	}

	verCmd := fixed[0]
	famProto := fixed[1]
	payloadLen := int(binary.BigEndian.Uint16(fixed[2:4]))

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return out, nil, err
		}
	}

	version := (verCmd & 0xF0) >> 4
	command := verCmd & 0x0F
	family := (famProto & 0xF0) >> 4
	protocol := famProto & 0x0F

	if version != 0x2 || command != 0x1 || protocol != 0x1 {
		return out, r, nil
	}

	switch family {
	case 0x1:
		if len(payload) < 12 {
			return out, r, nil
		}
		out.Valid = true
		out.SourceIP = net.IP(payload[0:4]).String()
		out.TargetIP = net.IP(payload[4:8]).String()
		out.SourcePort = int(binary.BigEndian.Uint16(payload[8:10]))
		out.TargetPort = int(binary.BigEndian.Uint16(payload[10:12]))
		return out, r, nil
	case 0x2:
		if len(payload) < 36 {
			return out, r, nil
		}
		out.Valid = true
		out.SourceIP = net.IP(payload[0:16]).String()
		out.TargetIP = net.IP(payload[16:32]).String()
		out.SourcePort = int(binary.BigEndian.Uint16(payload[32:34]))
		out.TargetPort = int(binary.BigEndian.Uint16(payload[34:36]))
		return out, r, nil
	default:
		return out, r, nil
	}
}

func forwardDecompress(dst net.Conn, src io.Reader, stats *trafficStats) error {
	zr, err := zstd.NewReader(&countingReader{r: src, cb: stats.addZstd})
	if err != nil {
		return err
	}
	defer zr.Close()

	buf := make([]byte, 16*1024)
	for {
		n, readErr := zr.Read(buf)
		if n > 0 {
			wrote, err := dst.Write(buf[:n])
			if wrote > 0 {
				stats.addRaw(wrote)
			}
			if err != nil {
				return err
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

func forwardCompress(dst io.Writer, src net.Conn, level int, flushInterval time.Duration, stats *trafficStats, perConnLimiter, globalLimiter *tokenBucket) error {
	encLevel := zstd.EncoderLevelFromZstd(level)
	limitedDst := newRateLimitedWriter(dst, perConnLimiter, globalLimiter)
	zw, err := zstd.NewWriter(&countingWriter{w: limitedDst, cb: stats.addZstd}, zstd.WithEncoderLevel(encLevel))
	if err != nil {
		return err
	}
	defer zw.Close()

	lastFlush := time.Now()
	shouldFlush := func(force bool) error {
		if flushInterval <= 0 {
			return zw.Flush()
		}
		if force || time.Since(lastFlush) >= flushInterval {
			lastFlush = time.Now()
			return zw.Flush()
		}
		return nil
	}

	buf := make([]byte, 16*1024)
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			stats.addRaw(n)
			if _, err := zw.Write(buf[:n]); err != nil {
				return err
			}
			if err := shouldFlush(false); err != nil {
				return err
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				if err := shouldFlush(true); err != nil {
					return err
				}
				return nil
			}
			return readErr
		}
	}
}

func startStatsPrinter(stop <-chan struct{}, stats *trafficStats) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var prevRaw uint64
	var prevZstd uint64
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			raw, zstdBytes, conns := stats.snapshot()
			rawDelta := raw - prevRaw
			zstdDelta := zstdBytes - prevZstd
			prevRaw = raw
			prevZstd = zstdBytes

			ratio := 0.0
			if raw > 0 {
				ratio = (float64(zstdBytes) / float64(raw)) * 100
			}

			fmt.Printf("[%s] Raw: %s (%s) | Zstd: %s (%s) | Ratio: %.2f%% | Conns: %d\n",
				time.Now().Format("15:04:05"),
				formatSize(raw),
				formatRate(rawDelta),
				formatSize(zstdBytes),
				formatRate(zstdDelta),
				ratio,
				conns)
		}
	}
}

func formatSize(n uint64) string {
	const unit = 1024.0
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	v := float64(n) / unit
	units := []string{"KB", "MB", "GB", "TB"}
	idx := 0
	for v >= unit && idx < len(units)-1 {
		v /= unit
		idx++
	}
	return fmt.Sprintf("%.2f %s", v, units[idx])
}

func formatRate(n uint64) string {
	const unit = 1024.0
	v := float64(n) / unit
	units := []string{"KB/s", "MB/s", "GB/s", "TB/s"}
	idx := 0
	for v >= unit && idx < len(units)-1 {
		v /= unit
		idx++
	}
	return fmt.Sprintf("%.1f%s", v, units[idx])
}

func closeWrite(conn net.Conn) {
	type closeWriter interface {
		CloseWrite() error
	}
	if cw, ok := conn.(closeWriter); ok {
		_ = cw.CloseWrite()
	}
}

func ipFromAddr(addr net.Addr) string {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

func isRealPipeErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return false
	}
	return true
}
