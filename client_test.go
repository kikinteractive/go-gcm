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

package fcm

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

func TestClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FCM Client")
}

var _ = Describe("FCM Client", func() {
	Describe("initializing", func() {
		DescribeTable("wrong initialization parameters",
			func(config *Config, h MessageHandler, errStr string) {
				c, err := NewClient(config, h)
				Expect(c).To(BeNil())
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(errStr))
			},
			Entry("it should fail on nil config", nil, nil, "config is nil"),
			Entry("it should fail on nil message handler",
				&Config{}, nil, "message handler is nil"),
			Entry("it should fail on empty sender id",
				&Config{}, func(cm CCSMessage) error { return nil }, "empty sender id"),
			Entry("it should fail on empty api key",
				&Config{SenderID: "123"}, func(cm CCSMessage) error { return nil }, "empty api key"),
			Entry("it should fail on wrong credentials",
				&Config{SenderID: "123", APIKey: "key"}, func(cm CCSMessage) error { return nil },
				"error connecting fcm xmpp client: auth failure: not-authorized"),
		)

		Context("good config", func() {
			var (
				xm *xmppCMock
				hm *httpCMock
			)

			BeforeEach(func() {
				xm = new(xmppCMock)
				hm = new(httpCMock)
			})

			AfterEach(func() {
				gt := GinkgoT()
				xm.AssertExpectations(gt)
				hm.AssertExpectations(gt)
			})

			It("should fail on xmpp connection error", func() {
				xm.On("ID").Return("id")
				xm.On("Listen", mock.AnythingOfType("fcm.MessageHandler")).
					Return(errors.New("Connect"))
				c, err := newFCMClient(xm, hm, &Config{}, nil)
				Expect(err).To(HaveOccurred())
				Expect(c).To(BeNil())
				Expect(err).To(MatchError("Connect"))
			})

			It("should succeed", func() {
				xm.On("ID").Return("id")
				xm.On("Listen", mock.AnythingOfType("fcm.MessageHandler")).
					Return(nil)
				c, err := newFCMClient(xm, hm, &Config{}, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(c).To(BeAssignableToTypeOf(&fcmClient{}))
				Expect(c.pingInterval).To(Equal(DefaultPingInterval))
				Expect(c.pingTimeout).To(Equal(DefaultPingTimeout))
				c.Lock()
				Expect(c.xmppClient).To(Equal(xm))
				Expect(c.cerr).NotTo(BeClosed())
				c.Unlock()
			})
		})
	})

	Describe("listening", func() {
		var (
			xm *xmppCMock
			c  *fcmClient
		)

		BeforeEach(func() {
			xm = new(xmppCMock)
			c = &fcmClient{
				xmppClient: xm,
				cerr:       make(chan error),
				senderID:   "sender id",
				apiKey:     "api key",
				mh:         nil,
			}
		})

		AfterEach(func() {
			xm.AssertExpectations(GinkgoT())
		})

		It("should fail on listen error", func() {
			xm.On("ID").Return("id")
			xm.On("Listen", mock.AnythingOfType("fcm.MessageHandler")).
				Return(errors.New("Listen"))
			go c.monitorXMPP(false)
			err := <-c.cerr
			Expect(err).To(MatchError("Listen"))
		})
	})

	Describe("sending", func() {
		var (
			xm XMPPMessage
			hm HTTPMessage
			h  *httpCMock
			x  *xmppCMock
			c  Client
		)

		BeforeEach(func() {
			xm = XMPPMessage{To: "me", MessageID: "id1"}
			hm = HTTPMessage{To: "me"}
			h = new(httpCMock)
			x = new(xmppCMock)
			c = &fcmClient{httpClient: h, xmppClient: x}
		})

		AfterEach(func() {
			gt := GinkgoT()
			h.AssertExpectations(gt)
			x.AssertExpectations(gt)
		})

		It("should send http message", func() {
			r := &HTTPResponse{MulticastID: 100}
			h.On("Send", hm).Return(r, nil)
			resp, err := c.SendHTTP(hm)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp).To(Equal(r))
		})

		It("should fail sending http message", func() {
			h.On("Send", hm).Return(nil, errors.New("send error"))
			resp, err := c.SendHTTP(hm)
			Expect(err).To(HaveOccurred())
			Expect(resp).To(BeNil())
			Expect(err).To(MatchError("send error"))
		})

		It("should send xmpp message", func() {
			x.On("Send", xm).Return("id1", 100, nil)
			id, bytes, err := c.SendXMPP(xm)
			Expect(err).NotTo(HaveOccurred())
			Expect(id).To(Equal("id1"))
			Expect(bytes).To(Equal(100))
		})

		It("should fail on send error", func() {
			x.On("Send", xm).Return("", 0, errors.New("send error"))
			id, bytes, err := c.SendXMPP(xm)
			Expect(err).To(HaveOccurred())
			Expect(id).To(BeEmpty())
			Expect(bytes).To(Equal(0))
			Expect(err).To(MatchError("send error"))
		})
	})

	Describe("closing", func() {
		var (
			x *xmppCMock
			c *fcmClient
		)

		BeforeEach(func() {
			x = new(xmppCMock)
			c = &fcmClient{xmppClient: x}
		})

		AfterEach(func() {
			x.AssertExpectations(GinkgoT())
		})

		It("should close successfully", func() {
			x.On("Close", true).Return(nil)
			x.On("IsClosed").Return(true)
			err := c.Close()
			Expect(err).NotTo(HaveOccurred())
			Expect(x.IsClosed()).To(BeTrue())
		})

		It("should return close error from xmpp", func() {
			x.On("Close", true).Return(errors.New("close error"))
			x.On("IsClosed").Return(true)
			err := c.Close()
			Expect(err).To(MatchError("close error"))
			Expect(x.IsClosed()).To(BeTrue())
		})
	})

	Describe("periodic ping", func() {
		var (
			x *xmppCMock
		)

		BeforeEach(func() {
			x = new(xmppCMock)
		})

		AfterEach(func() {
			x.AssertExpectations(GinkgoT())
		})

		It("should exit when xmpp client is closed", func() {
			x.On("IsClosed").Return(true)
			err := pingPeriodically(x, time.Millisecond, time.Millisecond)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should exit when xmpp ping errors", func() {
			x.On("IsClosed").Return(false)
			x.On("Ping", time.Millisecond).Return(errors.New("Ping"))
			err := pingPeriodically(x, time.Millisecond, time.Millisecond)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError("Ping"))
		})
	})

	Describe("handling upstream messages", func() {
		var (
			x *xmppCMock
			c *fcmClient
		)

		BeforeEach(func() {
			x = new(xmppCMock)
			c = &fcmClient{xmppClient: x, cerr: make(chan error, 1)}
		})

		AfterEach(func() {
			x.AssertExpectations(GinkgoT())
		})

		It("should handle connection drainging request", func() {
			cm := CCSMessage{
				MessageType: CCSControl,
				ControlType: "CONNECTION_DRAINING",
			}
			x.On("ID").Return("id")
			err := c.onCCSMessage(cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(c.cerr).To(HaveLen(1))
			err = <-c.cerr
			Expect(err).To(MatchError("connection draining"))
		})

		It("should bubble up everything else", func() {
			c.mh = func(cm CCSMessage) error {
				return errors.New("Bubble")
			}
			cm := CCSMessage{
				MessageType: CCSReceipt,
			}
			err := c.onCCSMessage(cm)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError("Bubble"))
		})
	})

	Describe("misc", func() {
		var (
			x *xmppCMock
			c *fcmClient
		)

		BeforeEach(func() {
			x = new(xmppCMock)
			c = &fcmClient{xmppClient: x}
		})

		AfterEach(func() {
			x.AssertExpectations(GinkgoT())
		})

		It("should return client id", func() {
			x.On("ID").Return("id1")
			Expect(c.ID()).To(Equal("id1"))
		})

		It("should return client jid", func() {
			x.On("JID").Return("jid")
			Expect(c.JID()).To(Equal("jid"))
		})
	})
})
