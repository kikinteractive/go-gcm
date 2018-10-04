// Package gcm provides send and receive GCM functionality.
package gcm

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/mattn/go-xmpp"
	"github.com/pborman/uuid"
)

const (
	// DefaultPingInterval is a default interval between pings.
	DefaultPingInterval = 20 * time.Second
	// DefaultPingTimeout is a default time to wait for ping replies.
	DefaultPingTimeout = 30 * time.Second

	// CCS XMPP message types.

	// CCSAck is a positive acknowledge.
	CCSAck = "ack"
	// CCSNack is a negative acknowledge.
	CCSNack = "nack"
	// CCSControl is a CCS upstream control message.
	CCSControl = "control"
	// CCSReceipt is an upstream receipt message.
	CCSReceipt = "receipt"

	// CCS XMPP control message types.

	// CCSDraining is an upstream CSS message it wants to drain the current connection.
	CCSDraining = "CONNECTION_DRAINING"

	// XMPP message types.

	// XMPPIQResult is a result for IQ query.
	XMPPIQResult = "result"

	// GCM service constants.
	ccsHostProd = "gcm.googleapis.com"
	ccsPortProd = "5235"
	ccsHostDev  = "gcm-preprod.googleapis.com"
	ccsPortDev  = "5236"

	// For CCS the min for exponential backoff has to be 1 sec
	ccsMinBackoff = 1 * time.Second

	// Formatter for outgoing stanzas.
	stanzaFmtStr = `<message id="%s"><gcm xmlns="google:mobile:data">%v</gcm></message>`
)

var (
	retryableErrors = map[string]bool{
		"Unavailable":           true,
		"SERVICE_UNAVAILABLE":   true,
		"InternalServerError":   true,
		"INTERNAL_SERVER_ERROR": true,
		// TODO(silvano): should we backoff with the same strategy on
		// DeviceMessageRateExceeded and TopicsMessageRateExceeded.
	}
)

// xmppClient is an interface to stub the internal xmpp.Client.
type xmppClient interface {
	JID() string
	Close() error
	IsEncrypted() bool
	Recv() (interface{}, error)
	Send(xmpp.Chat) (int, error)
	SendOrg(string) (int, error)
	PingC2S(string, string) error
}

// gcmXMPP is a container for the GCM XMPP Connection Server (CCS) client.
type gcmXMPP struct {
	sync.RWMutex
	xmppClient xmppClient
	xmppHost   string
	messages   struct {
		sync.RWMutex
		m map[string]*messageLogEntry
	}
	pongs      chan struct{}
	senderID   string
	closed     bool
	destructor sync.Once
	debug      bool
}

// An entry in the messages log, used to keep track of messages pending ack and
// retries for failed messages.
type messageLogEntry struct {
	body    *XMPPMessage
	backoff backoffProvider
}

// newXMPPClient creates a new client for GCM XMPP Server (CCS).
func newXMPPClient(isSandbox bool, senderID string, apiKey string, debug bool) (xmppC, error) {
	var xmppHost, xmppAddress string
	if isSandbox {
		xmppHost = ccsHostDev
		xmppAddress = net.JoinHostPort(ccsHostDev, ccsPortDev)
	} else {
		xmppHost = ccsHostProd
		xmppAddress = net.JoinHostPort(ccsHostProd, ccsPortProd)
	}

	nc, err := xmpp.NewClient(xmppAddress, xmppUser(xmppHost, senderID), apiKey, debug)
	if err != nil {
		return nil, fmt.Errorf("error connecting gcm xmpp client: %v", err)
	}

	xc := &gcmXMPP{
		xmppClient: nc,
		messages: struct {
			sync.RWMutex
			m map[string]*messageLogEntry
		}{
			m: make(map[string]*messageLogEntry),
		},
		xmppHost: xmppHost,
		senderID: senderID,
		pongs:    make(chan struct{}, 100),
		debug:    debug,
	}

	return xc, nil
}

// IsClosed reports if the client is already closed.
func (c *gcmXMPP) IsClosed() bool {
	return c.closed
}

// Ping sends a c2s ping message and blocks until s2c pong is received.
//
// Returns error if timeout time passes before pong.
func (c *gcmXMPP) Ping(timeout time.Duration) error {
	l := log.WithField("xmpp client id", c.ID())
	l.Debug("-- ping")
	// Guard wire access for thread safety.
	c.Lock()
	if err := c.xmppClient.PingC2S("", c.xmppHost); err != nil {
		c.Unlock()
		return err
	}
	c.Unlock()

	select {
	case <-c.pongs:
		// Ping successful.
		l.Debug("--- pong")
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("gcm xmpp pong timed out after %s", timeout.String())
	}
}

// Close sets the closing flag and (if graceful) waits until either all messages are
// processed or a timeout is reached.
func (c *gcmXMPP) Close(graceful bool) error {
	var err error
	l := log.WithFields(log.Fields{"gcm xmpp client id": c.ID(), "graceful": graceful})
	c.destructor.Do(func() {
		l.Debug("client close started")
		c.closed = true
		if graceful {
			// Wait until all is done, or timed out.
			select {
			case <-c.waitAllDone():
			case <-time.After(5 * time.Second):
				l.Debug("gcm xmpp taking a while to close, so giving up")
			}
		}
		err = c.xmppClient.Close()
		l.Debug("client closed")
	})

	return err
}

// ID returns the identifier of this XMPP client.
func (c *gcmXMPP) ID() string {
	return fmt.Sprintf("%p", c)
}

// JID returns the JID of this XMPP client.
func (c *gcmXMPP) JID() string {
	return c.xmppClient.JID()
}

// gcmXMPP implementation of listening for messages from CCS; the messages can be
// acks or nacks for messages sent through XMPP, control messages, upstream messages.
func (c *gcmXMPP) Listen(h MessageHandler) error {
	for {
		stanza, err := c.xmppClient.Recv()
		if err != nil {
			if c.closed {
				// Client is closed, return without error.
				break
			}
			// This is likely fatal, so return.
			return fmt.Errorf("error on Recv: %v", err)
		}
		switch v := stanza.(type) {
		case xmpp.Chat:
		case xmpp.IQ:
			// See if it's a pong and do not add more than one.
			if v.Type == XMPPIQResult && v.ID == "c2s1" && len(c.pongs) == 0 {
				c.pongs <- struct{}{}
			}
			continue
		case xmpp.Presence:
			continue
		}

		// OK, we are dealing with chat stanza.
		v := stanza.(xmpp.Chat)

		// Decode internal data if possible.
		var cm CCSMessage
		var decoded bool
		if len(v.Other) != 0 {
			err = json.Unmarshal([]byte(v.Other[0]), &cm)
			if err != nil {
				log.WithField("error", err).Warn("unmarshaling ccs message")
			} else {
				decoded = true
			}
		}

		switch v.Type {
		case "":
			// Successfully decoded CCSMessage is required.
			if !decoded {
				continue
			}
			switch cm.MessageType {
			case CCSAck:
				c.messages.Lock()
				// ack for a sent message, delete it from log.
				if _, ok := c.messages.m[cm.MessageID]; ok {
					go h(cm)
					delete(c.messages.m, cm.MessageID)
				}
				c.messages.Unlock()
			case CCSNack:
				// nack for a sent message, retry if retryable error, bubble up otherwise.
				if retryableErrors[cm.Error] {
					c.retryMessage(cm, h)
				} else {
					c.messages.Lock()
					if _, ok := c.messages.m[cm.MessageID]; ok {
						go h(cm)
						delete(c.messages.m, cm.MessageID)
					}
					c.messages.Unlock()
				}
			case CCSControl:
				log.WithField("ccs message", cm).Warn("control message")
				go h(cm)
			default:
				log.WithField("ccs message", cm).Warn("unknown ccs message type")
				go h(cm)
			}
		case "normal":
			// Successfully decoded CCSMessage is required.
			if !decoded {
				continue
			}
			switch cm.MessageType {
			case CCSReceipt:
				if c.debug {
					log.WithField("ccs message", cm).Debug("message receipt")
				}
				// Receipt message: send ack and pass to listener.
				ack := XMPPMessage{To: cm.From, MessageID: cm.MessageID, MessageType: CCSAck}
				c.Send(ack)
				go h(cm)
			default:
				log.WithField("ccs message", cm).Warn("uknown ccs message type")
				// Upstream message: send ack and pass to listener.
				ack := XMPPMessage{To: cm.From, MessageID: cm.MessageID, MessageType: CCSAck}
				c.Send(ack)
				go h(cm)
			}
		case "error":
			log.WithField("stanza", v).Warn("error stanza")
			// Successfully decoded CCSMessage is required.
			if !decoded {
				continue
			}
			go h(cm)
		default:
			log.WithField("stanza", v).Warn("unknown stanza")
		}
	}
	return nil
}

// TODO(silvano): add flow control (max 100 pending messages at one time)
// gcmXMPP implementation to send a message through GCM XMPP server (CCS).
func (c *gcmXMPP) Send(m XMPPMessage) (string, int, error) {
	if m.MessageID == "" {
		m.MessageID = uuid.New()
	}

	body, err := json.Marshal(m)
	if err != nil {
		return m.MessageID, 0, fmt.Errorf("could not unmarshal body of xmpp message: %v", err)
	}
	bs := string(body)

	payload := fmt.Sprintf(stanzaFmtStr, m.MessageID, bs)

	if c.debug {
		log.WithField("xmpp payload", payload).Debug("sending gcm xmpp")
	}

	// For Acks, just send the message.
	if m.MessageType == CCSAck {
		// Guard wire access for thread safety.
		c.Lock()
		bytes, err := c.xmppClient.SendOrg(payload)
		c.Unlock()
		return m.MessageID, bytes, err
	}

	// For payload messages, keep them until Ack/Nack is received.
	c.messages.Lock()
	if _, ok := c.messages.m[m.MessageID]; !ok {
		b := newExponentialBackoff()
		if b.getMin() < ccsMinBackoff {
			b.setMin(ccsMinBackoff)
		}
		c.messages.m[m.MessageID] = &messageLogEntry{body: &m, backoff: b}
	}
	c.messages.Unlock()

	// Guard wire access for thread safety.
	c.Lock()
	bytes, err := c.xmppClient.SendOrg(payload)
	c.Unlock()

	// If send is not successful, remove the message from the store immediately.
	if err != nil {
		c.messages.Lock()
		delete(c.messages.m, m.MessageID)
		c.messages.Unlock()
	}
	return m.MessageID, bytes, err
}

// waitAllDone waits until the message store is empty.
func (c *gcmXMPP) waitAllDone() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		for {
			// Exit if closed.
			if c.closed {
				break
			}
			// Exit if done.
			c.messages.RLock()
			nm := len(c.messages.m)
			c.messages.RUnlock()
			if nm == 0 {
				break
			}
			// Otherwise sleep and retry.
			time.Sleep(100 * time.Millisecond)
		}
		close(ch)
	}()
	return ch
}

// Retry sending an xmpp message with exponential backoff; if over limit, bubble up the failed message.
func (c *gcmXMPP) retryMessage(cm CCSMessage, h MessageHandler) {
	c.messages.RLock()
	defer c.messages.RUnlock()
	if me, ok := c.messages.m[cm.MessageID]; ok {
		if me.backoff.sendAnother() {
			go func(m *messageLogEntry) {
				m.backoff.wait()
				c.Send(*m.body)
			}(me)
		} else {
			log.WithField("xmpp payload", me).Warn("exponential backoff failed over limit")
			go h(cm)
		}
	}
}

// xmppUser generates an xmpp username from a sender ID.
func xmppUser(xmppHost, senderID string) string {
	return senderID + "@" + xmppHost
}
