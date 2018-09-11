package fcm

import (
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// httpC is an interface to stub the internal HTTP client.
type httpC interface {
	Send(m HTTPMessage) (*HTTPResponse, error)
}

// xmppC is an interface to stub the internal XMPP client.
type xmppC interface {
	Listen(h MessageHandler) error
	Send(m XMPPMessage) (string, int, error)
	Ping(timeout time.Duration) error
	Close(graceful bool) error
	IsClosed() bool
	ID() string
	JID() string
}

// fcmClient is a container for http and xmpp FCM clients.
type fcmClient struct {
	sync.RWMutex
	mh      MessageHandler
	cerr    chan error
	sandbox bool
	debug   bool
	// Clients.
	xmppClient xmppC
	httpClient httpC
	// FCM auth.
	senderID string
	apiKey   string
	// XMPP config.
	pingInterval time.Duration
	pingTimeout  time.Duration
}

// NewClient creates a new FCM client for this senderID.
func NewClient(config *Config, h MessageHandler) (Client, error) {
	switch {
	case config == nil:
		return nil, errors.New("config is nil")
	case h == nil:
		return nil, errors.New("message handler is nil")
	case config.SenderID == "":
		return nil, errors.New("empty sender id")
	case config.APIKey == "":
		return nil, errors.New("empty api key")
	}

	// Create FCM XMPP client.
	xmppc, err := newXMPPClient(config.Sandbox, config.SenderID, config.APIKey, config.Debug)
	if err != nil {
		return nil, err
	}

	// Create FCM HTTP client.
	httpc := newHTTPClient(config.APIKey, config.Debug)

	// Construct FCM client.
	return newFCMClient(xmppc, httpc, config, h)
}

// ID returns client unique identification.
func (c *fcmClient) ID() string {
	c.RLock()
	defer c.RUnlock()
	return c.xmppClient.ID()
}

// JID returns client XMPP JID.
func (c *fcmClient) JID() string {
	c.RLock()
	defer c.RUnlock()
	return c.xmppClient.JID()
}

// SendHTTP sends a message using the HTTP FCM connection server (blocking).
func (c *fcmClient) SendHTTP(m HTTPMessage) (*HTTPResponse, error) {
	return c.httpClient.Send(m)
}

// SendXMPP sends a message using the XMPP FCM connection server (blocking).
func (c *fcmClient) SendXMPP(m XMPPMessage) (string, int, error) {
	c.RLock()
	defer c.RUnlock()
	return c.xmppClient.Send(m)
}

// Close will stop and close the corresponding client, releasing all resources (blocking).
func (c *fcmClient) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.xmppClient != nil {
		return c.xmppClient.Close(true)
	}
	return nil
}

// newFCMClient creates an instance of fcmClient.
func newFCMClient(xmppc xmppC, httpc httpC, config *Config, h MessageHandler) (*fcmClient, error) {
	c := &fcmClient{
		httpClient:   httpc,
		xmppClient:   xmppc,
		cerr:         make(chan error, 1),
		senderID:     config.SenderID,
		apiKey:       config.APIKey,
		mh:           h,
		debug:        config.Debug,
		sandbox:      config.Sandbox,
		pingInterval: time.Duration(config.PingInterval) * time.Second,
		pingTimeout:  time.Duration(config.PingTimeout) * time.Second,
	}
	if c.pingInterval <= 0 {
		c.pingInterval = DefaultPingInterval
	}
	if c.pingTimeout <= 0 {
		c.pingTimeout = DefaultPingTimeout
	}

	// Create and monitor XMPP client.
	go c.monitorXMPP(config.MonitorConnection)

	// Wait a bit to see if the newly created client is ok.
	// TODO: find a better way (success notification, etc).
	select {
	case err := <-c.cerr:
		return nil, err
	case <-time.After(time.Second): // TODO: configurable
		// Looks good.
	}

	return c, nil

}

// monitorXMPP creates a new FCM XMPP client (if not provided), replaces the active client,
// closes the old client and starts monitoring the new connection.
func (c *fcmClient) monitorXMPP(activeMonitor bool) {
	firstRun := true
	for {
		var (
			xc   xmppC
			cerr chan error
		)

		// On the first run, use the provided client and error channel.
		if firstRun {
			cerr = c.cerr
			xc = c.xmppClient
		} else {
			xc = nil
			cerr = make(chan error, 1)
		}

		// Create XMPP client.
		log.WithField("sender id", c.senderID).Debug("creating fcm xmpp client")
		xmppc, err := connectXMPP(xc, c.sandbox, c.senderID, c.apiKey,
			c.onCCSMessage, cerr, c.debug)
		if err != nil {
			if firstRun {
				// On the first run, error exits the monitor.
				break
			}
			log.WithFields(log.Fields{"sender id": c.senderID, "error": err}).
				Error("connect fcm xmpp client")
			// Otherwise wait and try again.
			// TODO: remove infinite loop.
			time.Sleep(c.pingTimeout)
			continue
		}
		l := log.WithField("xmpp client ref", xmppc.ID())

		// New FCM XMPP client created and connected.
		if firstRun {
			l.Info("fcm xmpp client created")
			firstRun = false
		} else {
			// Replace the active client.
			c.Lock()
			prevc := c.xmppClient
			c.xmppClient = xmppc
			c.cerr = cerr
			c.Unlock()
			l.WithField("previous xmpp client ref", prevc.ID()).
				Warn("fcm xmpp client replaced")

			// Close the previous client.
			go prevc.Close(true)
		}

		// If active monitoring is enabled, start pinging routine.
		if activeMonitor {
			go func(xc xmppC, ce chan<- error) {
				// pingPeriodically is blocking.
				perr := pingPeriodically(xc, c.pingTimeout, c.pingInterval)
				if !xc.IsClosed() {
					ce <- perr
				}
			}(xmppc, cerr)
			l.Debug("fcm xmpp connection monitoring started")
		}

		// Wait for an error to occur (from listen, ping or upstream control).
		if err = <-cerr; err == nil {
			// No error, active close.
			break
		}

		l.WithField("error", err).Error("fcm xmpp connection")
		close(cerr)
	}
	log.WithField("sender id", c.senderID).
		Debug("fcm xmpp connection monitor finished")
}

// CCS upstream message callback.
// Tries to handle what it can here, before bubbling up.
func (c *fcmClient) onCCSMessage(cm CCSMessage) error {
	switch cm.MessageType {
	case CCSControl:
		// Handle connection drainging request.
		if cm.ControlType == CCSDraining {
			log.WithField("xmpp client ref", c.xmppClient.ID()).
				Warn("fcm xmpp connection draining requested")
			// Server should close the current connection.
			c.Lock()
			cerr := c.cerr
			c.Unlock()
			cerr <- errors.New("connection draining")
		}
		// Don't bubble up control messages.
		return nil
	}
	// Bubble up everything else.
	return c.mh(cm)
}

// Creates a new xmpp client (if not provided), connects to the server and starts listening.
func connectXMPP(c xmppC, isSandbox bool, senderID string, apiKey string,
	h MessageHandler, cerr chan<- error, debug bool) (xmppC, error) {
	var xmppc xmppC
	if c != nil {
		// Use the provided client.
		xmppc = c
	} else {
		// Create new.
		var err error
		xmppc, err = newXMPPClient(isSandbox, senderID, apiKey, debug)
		if err != nil {
			cerr <- err
			return nil, err
		}
	}

	l := log.WithField("xmpp client ref", xmppc.ID())

	// Start listening on this connection.
	go func() {
		l.Debug("fcm xmpp listen started")
		if err := xmppc.Listen(h); err != nil {
			l.WithField("error", err).Error("fcm xmpp listen")
			cerr <- err
		}
		l.Debug("fcm xmpp listen finished")
	}()

	return xmppc, nil
}

// pingPeriodically sends periodic pings. If pong is received, the timer is reset.
func pingPeriodically(xm xmppC, timeout, interval time.Duration) error {
	t := time.NewTimer(interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if xm.IsClosed() {
				return nil
			}
			if err := xm.Ping(timeout); err != nil {
				return err
			}
			t.Reset(interval)
		}
	}
}
