package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

// openAIWSIngressClientMaxLifetime bounds one physical downstream WebSocket.
// It is a var so lifecycle regressions can shrink it; production keeps the
// official websocket-mode 60-minute connection lifetime.
var openAIWSIngressClientMaxLifetime = 60 * time.Minute

// errOpenAIWSIngressLifetimeExceeded is the context cause used when the
// physical socket reaches openAIWSIngressClientMaxLifetime. The client read
// maps it to a graceful GoingAway close handshake, never a bare 1006.
var errOpenAIWSIngressLifetimeExceeded = errors.New("openai websocket maximum connection lifetime reached")

// OpenAIWSConnectionRegistry tracks the contexts of active downstream
// WebSocket connections so service shutdown can cancel them. HTTP
// Server.Shutdown does not close hijacked sockets, and HTTP-bridge streams
// would otherwise outlive an upstream pool close.
type OpenAIWSConnectionRegistry struct {
	mu     sync.Mutex
	next   uint64
	active map[uint64]context.CancelFunc
	closed bool
}

func NewOpenAIWSConnectionRegistry() *OpenAIWSConnectionRegistry {
	return &OpenAIWSConnectionRegistry{active: make(map[uint64]context.CancelFunc)}
}

// Register derives a child context cancelled on Close and returns it with an
// idempotent unregister func. Registering after Close yields an
// already-cancelled context so late callers unwind immediately.
func (r *OpenAIWSConnectionRegistry) Register(ctx context.Context) (context.Context, func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil {
		return ctx, func() {}
	}
	child, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		cancel()
		return child, func() {}
	}
	r.next++
	id := r.next
	r.active[id] = cancel
	r.mu.Unlock()
	var once sync.Once
	return child, func() {
		once.Do(func() {
			r.mu.Lock()
			if r.active != nil {
				delete(r.active, id)
			}
			r.mu.Unlock()
		})
	}
}

// Close cancels every registered context and rejects later registrations.
// It is safe to call multiple times.
func (r *OpenAIWSConnectionRegistry) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	cancels := make([]context.CancelFunc, 0, len(r.active))
	for id, cancel := range r.active {
		cancels = append(cancels, cancel)
		delete(r.active, id)
	}
	r.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (s *OpenAIGatewayService) openaiWSConnectionRegistry() *OpenAIWSConnectionRegistry {
	if s == nil {
		return nil
	}
	s.openaiWSConnectionsOnce.Do(func() {
		if s.openaiWSConnections == nil {
			s.openaiWSConnections = NewOpenAIWSConnectionRegistry()
		}
	})
	return s.openaiWSConnections
}

// RegisterOpenAIWSConnection registers one downstream WebSocket connection
// context for shutdown cancellation. The caller owns the returned unregister
// func and must call it when the connection ends.
func (s *OpenAIGatewayService) RegisterOpenAIWSConnection(ctx context.Context) (context.Context, func()) {
	registry := s.openaiWSConnectionRegistry()
	if registry == nil {
		if ctx == nil {
			ctx = context.Background()
		}
		return ctx, func() {}
	}
	return registry.Register(ctx)
}

// closeOpenAIWSRegisteredConnections cancels every registered downstream
// connection context. CloseOpenAIWSPool calls this even when the pool is nil
// so hijacked sockets and HTTP-bridge streams cannot outlive the service.
func (s *OpenAIGatewayService) closeOpenAIWSRegisteredConnections() {
	registry := s.openaiWSConnectionRegistry()
	if registry != nil {
		registry.Close()
	}
}

type openAIWSClientReadResult struct {
	messageType coderws.MessageType
	payload     []byte
	err         error
}

type openAIWSPhysicalClientKey struct{}

// One reader and one lifetime belong to the physical socket, not to a relay
// attempt. Canceling a failed attempt releases its waiter without closing the
// socket or leaving a reader that can steal the next attempt's client frame.
type openAIWSPhysicalClient struct {
	conn        *coderws.Conn
	ctx         context.Context
	parent      context.Context
	cancel      context.CancelCauseFunc
	unregister  func()
	reads       chan openAIWSClientReadResult
	readFailure chan error
	readErr     error // Published by closing readerDone.
	readerDone  chan struct{}
	watchDone   chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once
	queuedBytes atomic.Int64
	lifetime    time.Duration
	scope       openAIWSConnectionScope
}

func openAIWSPhysicalClientGinKey(conn *coderws.Conn) string {
	return fmt.Sprintf("openai_ws_physical_client_%p", conn)
}

func openAIWSPhysicalClientFromGin(c *gin.Context, conn *coderws.Conn) *openAIWSPhysicalClient {
	if c == nil {
		return nil
	}
	value, _ := c.Get(openAIWSPhysicalClientGinKey(conn))
	client, _ := value.(*openAIWSPhysicalClient)
	return client
}

func (s *OpenAIGatewayService) startOpenAIWSPhysicalClient(ctx, lifecycleCtx context.Context, c *gin.Context, conn *coderws.Conn) *openAIWSPhysicalClient {
	if client := openAIWSPhysicalClientFromGin(c, conn); client != nil {
		return client
	}
	parent := lifecycleCtx
	if parent == nil {
		parent = ctx
	}
	parent, unregister := s.RegisterOpenAIWSConnection(parent)
	physicalCtx, cancel := context.WithCancelCause(context.WithoutCancel(parent))
	client := &openAIWSPhysicalClient{
		conn: conn, ctx: physicalCtx, parent: parent, cancel: cancel, unregister: unregister,
		reads: make(chan openAIWSClientReadResult, 32), readFailure: make(chan error, 1),
		readerDone: make(chan struct{}), watchDone: make(chan struct{}), closed: make(chan struct{}),
		lifetime: openAIWSIngressClientMaxLifetime,
	}
	if scope, ok := ctx.Value(openAIWSConnectionScopeKey{}).(openAIWSConnectionScope); ok {
		client.scope = scope
		if !scope.startedAt.IsZero() {
			client.lifetime -= time.Since(scope.startedAt)
		}
	}
	if c != nil {
		c.Set(openAIWSPhysicalClientGinKey(conn), client)
	}
	go client.readPump()
	go client.watch(parent)
	return client
}

// StartOpenAIWSClientConnection is called once before native account selection.
// The returned context preserves request values and follows the socket lifetime.
func (s *OpenAIGatewayService) StartOpenAIWSClientConnection(ctx, lifecycleCtx context.Context, c *gin.Context, conn *coderws.Conn) (context.Context, func()) {
	client := s.startOpenAIWSPhysicalClient(ctx, lifecycleCtx, c, conn)
	return client.executionContext(ctx)
}

// OpenAIWSClientConnectionContext transfers only the physical reader/lifetime
// into a clean bridge context when a native attempt switches to HTTP.
func (s *OpenAIGatewayService) OpenAIWSClientConnectionContext(ctx context.Context, c *gin.Context, conn *coderws.Conn) (context.Context, func()) {
	if client := openAIWSPhysicalClientFromGin(c, conn); client != nil {
		return client.executionContext(ctx)
	}
	return ctx, func() {}
}

func (client *openAIWSPhysicalClient) executionContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(client.ctx, func() { cancel(context.Cause(client.ctx)) })
	if client.ctx.Err() != nil {
		cancel(context.Cause(client.ctx))
	}
	if client.scope.connectionID != "" {
		ctx = context.WithValue(ctx, openAIWSConnectionScopeKey{}, client.scope)
	}
	return context.WithValue(ctx, openAIWSPhysicalClientKey{}, client), func() {
		stop()
		cancel(context.Canceled)
	}
}

func (s *OpenAIGatewayService) CloseOpenAIWSClientConnection(c *gin.Context, conn *coderws.Conn) {
	if client := openAIWSPhysicalClientFromGin(c, conn); client != nil {
		if client.parent.Err() != nil {
			client.close(coderws.StatusGoingAway, "websocket request canceled", context.Cause(client.parent))
		} else {
			client.close(coderws.StatusNormalClosure, "websocket connection complete", context.Canceled)
		}
		<-client.readerDone
		<-client.watchDone
	}
}

func (client *openAIWSPhysicalClient) close(status coderws.StatusCode, reason string, cause error) {
	client.closeOnce.Do(func() {
		client.cancel(NewOpenAIWSClientCloseError(status, reason, cause))
		_ = client.conn.Close(status, reason)
		_ = client.conn.CloseNow()
		<-client.readerDone
	drain:
		for {
			select {
			case <-client.reads:
			default:
				break drain
			}
		}
		client.queuedBytes.Store(0)
		client.unregister()
		close(client.closed)
	})
}

func (client *openAIWSPhysicalClient) readPump() {
	defer close(client.readerDone)
	for {
		kind, payload, err := client.conn.Read(context.Background())
		if err != nil {
			client.readErr = err
			client.readFailure <- err
			return
		}
		queued := client.queuedBytes.Add(int64(len(payload)))
		if queued <= 64<<20 {
			select {
			case client.reads <- openAIWSClientReadResult{messageType: kind, payload: payload}:
				continue
			case <-client.ctx.Done():
				return
			default:
			}
		}
		client.readErr = NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "websocket client queue capacity exceeded", nil)
		client.readFailure <- client.readErr
		return
	}
}

func (client *openAIWSPhysicalClient) watch(parent context.Context) {
	defer close(client.watchDone)
	lifetime := time.NewTimer(client.lifetime)
	defer lifetime.Stop()
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	pingResult := make(chan error, 1)
	pinging := false
	peerDisconnected := false
	var pings sync.WaitGroup
	defer pings.Wait()
	for {
		select {
		case <-client.closed:
			return
		case <-parent.Done():
			status, reason := coderws.StatusGoingAway, "websocket request canceled"
			if errors.Is(context.Cause(parent), ErrOpenAIWSIngressLeaseLost) {
				status, reason = coderws.StatusTryAgainLater, "websocket ingress capacity lease lost; please reconnect"
			}
			client.close(status, reason, context.Cause(parent))
			return
		case <-lifetime.C:
			client.close(coderws.StatusGoingAway, "maximum websocket connection lifetime reached; please reconnect", errOpenAIWSIngressLifetimeExceeded)
			return
		case err := <-client.readFailure:
			var closeErr *OpenAIWSClientCloseError
			if errors.As(err, &closeErr) {
				client.close(closeErr.StatusCode(), closeErr.Reason(), err)
				return
			}
			// Preserve the native path's bounded terminal-usage drain after a
			// peer disconnect. Readers see the error immediately; existing
			// upstream drain limits, shutdown and the hard lifetime still apply.
			peerDisconnected = true
			ticker.Stop()
		case <-ticker.C:
			if !pinging {
				pinging = true
				pings.Add(1)
				go func() {
					defer pings.Done()
					ctx, cancel := context.WithTimeout(client.ctx, 10*time.Second)
					defer cancel()
					pingResult <- client.conn.Ping(ctx)
				}()
			}
		case err := <-pingResult:
			pinging = false
			if err != nil && !peerDisconnected {
				client.close(coderws.StatusGoingAway, "websocket heartbeat timeout", err)
				return
			}
		}
	}
}

func (client *openAIWSPhysicalClient) read(ctx context.Context, timeout time.Duration, timeoutStatus coderws.StatusCode, timeoutReason string, timeoutStart <-chan struct{}, timeoutActive func() bool) (coderws.MessageType, []byte, error) {
	readCancellation := func() error {
		if client.ctx.Err() != nil {
			return context.Cause(client.ctx)
		}
		if client.parent.Err() != nil {
			return NewOpenAIWSClientCloseError(coderws.StatusGoingAway, "websocket request canceled", context.Cause(client.parent))
		}
		return context.Cause(ctx)
	}
	if ctx.Err() != nil {
		return 0, nil, readCancellation()
	}
	var timer *time.Timer
	var timeoutCh <-chan time.Time
	startTimeout := func() {
		if timeout <= 0 || (timeoutActive != nil && !timeoutActive()) {
			return
		}
		if timer == nil {
			timer = time.NewTimer(timeout)
		} else {
			timer.Reset(timeout)
		}
		timeoutCh = timer.C
	}
	startTimeout()
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case frame := <-client.reads:
			client.queuedBytes.Add(-int64(len(frame.payload)))
			return frame.messageType, frame.payload, frame.err
		case <-ctx.Done():
			return 0, nil, readCancellation()
		case <-client.ctx.Done():
			return 0, nil, context.Cause(client.ctx)
		case <-client.readerDone:
			return 0, nil, client.readErr
		case <-timeoutStart:
			startTimeout()
		case <-timeoutCh:
			if timeoutActive != nil && !timeoutActive() {
				timeoutCh = nil
				continue
			}
			client.close(timeoutStatus, timeoutReason, context.DeadlineExceeded)
			return 0, nil, context.Cause(client.ctx)
		}
	}
}

// ReadOpenAIWSClientMessage keeps one reader alive while control events send
// their close frame, then closes the transport and joins that reader.
func ReadOpenAIWSClientMessage(
	controlCtx context.Context,
	conn *coderws.Conn,
	timeout time.Duration,
	timeoutStatus coderws.StatusCode,
	timeoutReason string,
) (coderws.MessageType, []byte, error) {
	return readOpenAIWSClientMessageWithTimeoutStart(
		controlCtx,
		conn,
		timeout,
		timeoutStatus,
		timeoutReason,
		nil,
		nil,
	)
}

// readOpenAIWSClientMessageWithTimeoutStart supports readers whose timeout
// starts after a state transition, such as a completed passthrough turn. When
// timeoutActive is nil, a positive timeout starts immediately.
func readOpenAIWSClientMessageWithTimeoutStart(
	controlCtx context.Context,
	conn *coderws.Conn,
	timeout time.Duration,
	timeoutStatus coderws.StatusCode,
	timeoutReason string,
	timeoutStart <-chan struct{},
	timeoutActive func() bool,
) (coderws.MessageType, []byte, error) {
	if conn == nil {
		return 0, nil, errors.New("openai websocket client connection is nil")
	}
	if controlCtx == nil {
		controlCtx = context.Background()
	}
	if client, ok := controlCtx.Value(openAIWSPhysicalClientKey{}).(*openAIWSPhysicalClient); ok && client.conn == conn {
		kind, payload, err := client.read(controlCtx, timeout, timeoutStatus, timeoutReason, timeoutStart, timeoutActive)
		if err != nil && errors.Is(context.Cause(controlCtx), ErrOpenAIWSIngressLeaseLost) {
			client.close(coderws.StatusTryAgainLater, "websocket ingress capacity lease lost; please reconnect", ErrOpenAIWSIngressLeaseLost)
			return 0, nil, context.Cause(client.ctx)
		}
		return kind, payload, err
	}

	readDone := make(chan openAIWSClientReadResult, 1)
	go func() {
		messageType, payload, err := conn.Read(context.Background())
		readDone <- openAIWSClientReadResult{messageType: messageType, payload: payload, err: err}
	}()

	var timer *time.Timer
	var timeoutCh <-chan time.Time
	startTimeout := func() {
		if timeout <= 0 || (timeoutActive != nil && !timeoutActive()) {
			return
		}
		if timer == nil {
			timer = time.NewTimer(timeout)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
		}
		timeoutCh = timer.C
	}
	if timeoutActive == nil || timeoutActive() {
		startTimeout()
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	closeAndJoin := func(status coderws.StatusCode, reason string, cause error) (coderws.MessageType, []byte, error) {
		_ = conn.Close(status, reason)
		_ = conn.CloseNow()
		<-readDone
		return 0, nil, NewOpenAIWSClientCloseError(status, reason, cause)
	}

	for {
		select {
		case result := <-readDone:
			return result.messageType, result.payload, result.err
		case <-timeoutStart:
			startTimeout()
		case <-timeoutCh:
			return closeAndJoin(timeoutStatus, timeoutReason, context.DeadlineExceeded)
		case <-controlCtx.Done():
			cause := context.Cause(controlCtx)
			if errors.Is(cause, ErrOpenAIWSIngressLeaseLost) {
				return closeAndJoin(
					coderws.StatusTryAgainLater,
					"websocket ingress capacity lease lost; please reconnect",
					cause,
				)
			}
			if errors.Is(cause, errOpenAIWSIngressLifetimeExceeded) {
				return closeAndJoin(
					coderws.StatusGoingAway,
					"maximum websocket connection lifetime reached; please reconnect",
					cause,
				)
			}
			return closeAndJoin(coderws.StatusGoingAway, "websocket request canceled", cause)
		}
	}
}
