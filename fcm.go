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

// Package fcm provides send and receive FCM functionality.
package fcm

// HTTPMessage defines a downstream FCM HTTP message.
type HTTPMessage struct {
	To                    string        `json:"to,omitempty"`
	RegistrationIDs       []string      `json:"registration_ids,omitempty"`
	CollapseKey           string        `json:"collapse_key,omitempty"`
	Priority              string        `json:"priority,omitempty"`
	ContentAvailable      bool          `json:"content_available,omitempty"`
	TimeToLive            *uint         `json:"time_to_live,omitempty"`
	RestrictedPackageName string        `json:"restricted_package_name,omitempty"`
	DryRun                bool          `json:"dry_run,omitempty"`
	Data                  Data          `json:"data,omitempty"`
	Notification          *Notification `json:"notification,omitempty"`
}

// HTTPResponse is the FCM connection server response to an HTTP downstream message.
type HTTPResponse struct {
	StatusCode   int          `json:"-"`
	MulticastID  int64        `json:"multicast_id"`
	Success      uint         `json:"success"`
	Failure      uint         `json:"failure"`
	CanonicalIds uint         `json:"canonical_ids"`
	Results      []HTTPResult `json:"results,omitempty"`
	// Topic message HTTP response only.
	MessageID uint   `json:"message_id,omitempty"`
	Error     string `json:"error,omitempty"`
}

// HTTPResult represents the result of a single processed HTTP request.
type HTTPResult struct {
	MessageID      string `json:"message_id,omitempty"`
	RegistrationID string `json:"registration_id,omitempty"`
	Error          string `json:"error,omitempty"`
}

// XMPPMessage defines a downstream FCM XMPP message.
type XMPPMessage struct {
	To                       string        `json:"to,omitempty"`
	MessageID                string        `json:"message_id"`
	MessageType              string        `json:"message_type,omitempty"`
	CollapseKey              string        `json:"collapse_key,omitempty"`
	Priority                 string        `json:"priority,omitempty"`
	ContentAvailable         bool          `json:"content_available,omitempty"`
	TimeToLive               *uint         `json:"time_to_live,omitempty"`
	DeliveryReceiptRequested bool          `json:"delivery_receipt_requested,omitempty"`
	DryRun                   bool          `json:"dry_run,omitempty"`
	Data                     Data          `json:"data,omitempty"`
	Notification             *Notification `json:"notification,omitempty"`
}

// Data defines the custom payload of a FCM message.
type Data map[string]interface{}

// Notification defines the notification payload of a FCM message.
// NOTE: contains keys for both Android and iOS notifications.
type Notification struct {
	// Common fields.
	Title        string `json:"title,omitempty"`
	Body         string `json:"body,omitempty"`
	Sound        string `json:"sound,omitempty"`
	ClickAction  string `json:"click_action,omitempty"`
	BodyLocKey   string `json:"body_loc_key,omitempty"`
	BodyLocArgs  string `json:"body_loc_args,omitempty"`
	TitleLocKey  string `json:"title_loc_key,omitempty"`
	TitleLocArgs string `json:"title_loc_args,omitempty"`

	// Android-only fields.
	Icon  string `json:"icon,omitempty"`
	Tag   string `json:"tag,omitempty"`
	Color string `json:"color,omitempty"`

	// iOS-only fields
	Badge string `json:"badge,omitempty"`
}

// Config is a container for FCM client configuration data.
type Config struct {
	SenderID          string `json:"sender_id"`
	APIKey            string `json:"api_key"`
	Sandbox           bool   `json:"sandbox"`
	MonitorConnection bool   `json:"monitor_connection"`
	Debug             bool   `json:"debug"`
	PingInterval      int    `json:"ping_interval"`
	PingTimeout       int    `json:"ping_timeout"`
}

// CCSMessage is an XMPP message sent from CCS.
// The CCS message can be an upstream message (device to server) or a
// message from CCS (e.g. a delivery receipt, a control, etc).
type CCSMessage struct {
	From             string `json:"from, omitempty"`
	MessageID        string `json:"message_id, omitempty"`
	MessageType      string `json:"message_type, omitempty"`
	RegistrationID   string `json:"registration_id,omitempty"`
	Error            string `json:"error,omitempty"`
	ErrorDescription string `json:"error_description,omitempty"`
	Category         string `json:"category, omitempty"`
	Data             Data   `json:"data,omitempty"`
	ControlType      string `json:"control_type,omitempty"`
}

// MessageHandler is the type for a function that handles a CCS message.
type MessageHandler func(cm CCSMessage) error

// Client defines an interface for the FCM client.
type Client interface {
	ID() string
	SendHTTP(m HTTPMessage) (*HTTPResponse, error)
	SendXMPP(m XMPPMessage) (string, int, error)
	Close() error
}
