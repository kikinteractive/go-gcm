// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package gcm provides send and receive GCM functionality.
package gcm

import (
	"log"
	"time"

	"github.com/jpillora/backoff"
)

var (
	// DebugMode determines whether to have verbose logging.
	DebugMode = true
	// Default Min and Max delay for backoff.
	DefaultMinBackoff   = 1 * time.Second
	DefaultMaxBackoff   = 10 * time.Second
	DefaultPingInterval = 10 * time.Second
	DefaultPingTimeout  = 5 * time.Second
)

// Prints debug info if DebugMode is set.
func debug(m string, v interface{}) {
	if DebugMode {
		log.Printf(m+":%+v", v)
	}
}

// The data payload of a GCM message.
type Data map[string]interface{}

// The notification payload of a GCM message.
type Notification struct {
	Title        string `json:"title,omitempty"`
	Body         string `json:"body,omitempty"`
	Icon         string `json:"icon,omitempty"`
	Sound        string `json:"sound,omitempty"`
	Badge        string `json:"badge,omitempty"`
	Tag          string `json:"tag,omitempty"`
	Color        string `json:"color,omitempty"`
	ClickAction  string `json:"click_action,omitempty"`
	BodyLocKey   string `json:"body_loc_key,omitempty"`
	BodyLocArgs  string `json:"body_loc_args,omitempty"`
	TitleLocArgs string `json:"title_loc_args,omitempty"`
	TitleLocKey  string `json:"title_loc_key,omitempty"`
}

type Client struct {
	senderID   string
	apiKey     string
	mh         MessageHandler
	xmppClient *xmppGcmClient
	httpClient *httpGcmClient
}

func NewClient(senderID string, apiKey string, h MessageHandler) (*Client, error) {
	ht := newHttpGcmClient(apiKey)
	xm, err := connectXmpp(senderID, apiKey, h)
	if err != nil {
		return nil, err
	}

	c := &Client{
		senderID:   senderID,
		apiKey:     apiKey,
		mh:         h,
		xmppClient: xm,
		httpClient: ht,
	}

	// Ping periodically and indentify xmpp disconnect.
	go func() {
		for {
			if err := c.xmppClient.pingPeriodically(DefaultPingTimeout, DefaultPingInterval); err != nil {
				newc, err := connectXmpp(senderID, apiKey, h)
				if err != nil {
					debug("error creating xmpp client> %s", err)
					time.Sleep(DefaultPingInterval)
					continue
				}
				c.xmppClient = newc
			}
		}
	}()

	return c, nil
}

// Send a message using the HTTP GCM connection server.
func (c *Client) SendHttp(m HttpMessage) (*HttpResponse, error) {
	b := newExponentialBackoff()
	return c.httpClient.sendHttp(m, b)
}

// SendXmpp sends a message using the XMPP GCM connection server.
func (c *Client) SendXmpp(m XmppMessage) (string, int, error) {
	return c.xmppClient.send(m)
}

// Close will stop and close the corresponding client.
func (c *Client) Close() error {
	c.xmppClient.gracefulClose()
	return nil
}

func connectXmpp(senderID string, apiKey string, h MessageHandler) (*xmppGcmClient, error) {
	x, err := newXmppGcmClient(senderID, apiKey)
	if err != nil {
		return nil, err
	}

	// Start listening on this connection.
	go func() {
		if err := x.listen(h); err != nil {
			// Pass the error upstream.
			//c.cerr <- err
			debug("gcm listen error %v", err)
		}
		debug("gcm listen finished", "")
	}()

	return x, nil
}

// Implementation of backoff provider using exponential backoff.
type exponentialBackoff struct {
	b            backoff.Backoff
	currentDelay time.Duration
}

// Factory method for exponential backoff, uses default values for Min and Max and
// adds Jitter.
func newExponentialBackoff() *exponentialBackoff {
	b := &backoff.Backoff{
		Min:    DefaultMinBackoff,
		Max:    DefaultMaxBackoff,
		Jitter: true,
	}
	return &exponentialBackoff{b: *b, currentDelay: b.Duration()}
}

// Returns true if not over the retries limit
func (eb exponentialBackoff) sendAnother() bool {
	return eb.currentDelay <= eb.b.Max
}

// Set the minumim delay for backoff
func (eb *exponentialBackoff) setMin(min time.Duration) {
	eb.b.Min = min
	if (eb.currentDelay) < min {
		eb.currentDelay = min
	}
}

// Wait for the current value of backoff
func (eb exponentialBackoff) wait() {
	time.Sleep(eb.currentDelay)
	eb.currentDelay = eb.b.Duration()
}
