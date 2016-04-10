// Package gcm provides send and receive GCM functionality.
package gcm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	httpAddress = "https://android.googleapis.com/gcm/send"
)

// A GCM Http message.
type HttpMessage struct {
	To                    string        `json:"to,omitempty"`
	RegistrationIds       []string      `json:"registration_ids,omitempty"`
	CollapseKey           string        `json:"collapse_key,omitempty"`
	Priority              string        `json:"priority,omitempty"`
	ContentAvailable      bool          `json:"content_available,omitempty"`
	DelayWhileIdle        bool          `json:"delay_while_idle,omitempty"`
	TimeToLive            uint          `json:"time_to_live,omitempty"`
	RestrictedPackageName string        `json:"restricted_package_name,omitempty"`
	DryRun                bool          `json:"dry_run,omitempty"`
	Data                  Data          `json:"data,omitempty"`
	Notification          *Notification `json:"notification,omitempty"`
}

// HttpResponse is the GCM connection server response to an HTTP downstream message request.
type HttpResponse struct {
	MulticastId  int      `json:"multicast_id,omitempty"`
	Success      uint     `json:"success,omitempty"`
	Failure      uint     `json:"failure,omitempty"`
	CanonicalIds uint     `json:"canonical_ids,omitempty"`
	Results      []Result `json:"results,omitempty"`
	MessageId    uint     `json:"message_id,omitempty"`
	Error        string   `json:"error,omitempty"`
}

// Result represents the status of a processed Http message.
type Result struct {
	MessageId      string `json:"message_id,omitempty"`
	RegistrationId string `json:"registration_id,omitempty"`
	Error          string `json:"error,omitempty"`
}

// Used to compute results for multicast messages with retries.
type multicastResultsState map[string]*Result

// httpClient is an interface to stub the http client in tests.
type httpClient interface {
	send(apiKey string, m HttpMessage) (*HttpResponse, error)
	getRetryAfter() string
}

// httpGcmClient is a client for the Gcm Http Connection Server.
type httpGcmClient struct {
	GcmURL     string
	apiKey     string
	HttpClient *http.Client
	retryAfter string
}

// Interface to stub the http client in tests.
type backoffProvider interface {
	sendAnother() bool
	setMin(min time.Duration)
	wait()
}

func newHttpGcmClient(apiKey string) *httpGcmClient {
	return &httpGcmClient{
		GcmURL:     httpAddress,
		apiKey:     apiKey,
		HttpClient: &http.Client{},
		retryAfter: "0",
	}
}

// Get the value of the retry after header if present.
func (c httpGcmClient) getRetryAfter() string {
	return c.retryAfter
}

// sendHttp sends an http message using exponential backoff, handling multicast replies.
func (c *httpGcmClient) sendHttp(m HttpMessage, b backoffProvider) (*HttpResponse, error) {
	// TODO(silvano): check this with responses for topic/notification group
	gcmResp := &HttpResponse{}
	var multicastId int
	targets, err := messageTargetAsStringsArray(m)
	if err != nil {
		return gcmResp, fmt.Errorf("error extracting target from message: %v", err)
	}
	// make a copy of the targets to keep track of results during retries
	localTo := make([]string, len(targets))
	copy(localTo, targets)
	resultsState := &multicastResultsState{}
	for b.sendAnother() {
		gcmResp, err = c.send(m)
		if err != nil {
			return gcmResp, fmt.Errorf("error sending request to GCM HTTP server: %v", err)
		}
		if len(gcmResp.Results) > 0 {
			doRetry, toRetry, err := checkResults(gcmResp.Results, localTo, *resultsState)
			multicastId = gcmResp.MulticastId
			if err != nil {
				return gcmResp, fmt.Errorf("error checking GCM results: %v", err)
			}
			if doRetry {
				retryAfter, err := time.ParseDuration(c.getRetryAfter())
				if err != nil {
					b.setMin(retryAfter)
				}
				localTo = make([]string, len(toRetry))
				copy(localTo, toRetry)
				if m.RegistrationIds != nil {
					m.RegistrationIds = toRetry
				}
				b.wait()
				continue
			} else {
				break
			}
		} else {
			break
		}
	}
	// if it was multicast, reconstruct response in case there have been retries
	if len(targets) > 1 {
		gcmResp = buildRespForMulticast(targets, *resultsState, multicastId)
	}
	return gcmResp, nil
}

// httpGcmClient implementation to send a message through GCM Http server.
func (c *httpGcmClient) send(m HttpMessage) (*HttpResponse, error) {
	bs, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("error marshalling message>%v", err)
	}
	debug("sending", string(bs))
	req, err := http.NewRequest("POST", c.GcmURL, bytes.NewReader(bs))
	if err != nil {
		return nil, fmt.Errorf("error creating request>%v", err)
	}
	req.Header.Add(http.CanonicalHeaderKey("Content-Type"), "application/json")
	req.Header.Add(http.CanonicalHeaderKey("Authorization"), fmt.Sprintf("key=%v", c.apiKey))
	httpResp, err := c.HttpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error sending request to HTTP connection server>%v", err)
	}
	gcmResp := &HttpResponse{}
	body, err := ioutil.ReadAll(httpResp.Body)
	defer httpResp.Body.Close()
	if err != nil {
		return gcmResp, fmt.Errorf("error reading http response body>%v", err)
	}
	debug("received body", string(body))
	err = json.Unmarshal(body, &gcmResp)
	if err != nil {
		return gcmResp, fmt.Errorf("error unmarshaling json from body: %v", err)
	}
	// TODO(silvano): this is assuming that the header contains seconds instead of a date, need to check
	c.retryAfter = httpResp.Header.Get(http.CanonicalHeaderKey("Retry-After"))
	return gcmResp, nil
}

// Builds the final response for a multicast message, in case there have been retries for
// subsets of the original recipients.
func buildRespForMulticast(to []string, mrs multicastResultsState, mid int) *HttpResponse {
	resp := &HttpResponse{}
	resp.MulticastId = mid
	resp.Results = make([]Result, len(to))
	for i, regId := range to {
		result, ok := mrs[regId]
		if !ok {
			continue
		}
		resp.Results[i] = *result
		if result.MessageId != "" {
			resp.Success++
		} else if result.Error != "" {
			resp.Failure++
		}
		if result.RegistrationId != "" {
			resp.CanonicalIds++
		}
	}
	return resp
}

// Transform the recipient in an array of strings if needed.
func messageTargetAsStringsArray(m HttpMessage) ([]string, error) {
	if m.RegistrationIds != nil {
		return m.RegistrationIds, nil
	} else if m.To != "" {
		target := []string{m.To}
		return target, nil
	}
	target := []string{}
	return target, fmt.Errorf("can't find any valid target field in message.")
}

// For a multicast send, determines which errors can be retried.
func checkResults(gcmResults []Result, recipients []string, resultsState multicastResultsState) (doRetry bool, toRetry []string, err error) {
	doRetry = false
	toRetry = []string{}
	for i := 0; i < len(gcmResults); i++ {
		result := gcmResults[i]
		regId := recipients[i]
		resultsState[regId] = &result
		if result.Error != "" {
			if retryableErrors[result.Error] {
				toRetry = append(toRetry, regId)
				if doRetry == false {
					doRetry = true
				}
			}
		}
	}
	return doRetry, toRetry, nil
}
