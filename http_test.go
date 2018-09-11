package fcm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("HTTP Client", func() {
	var (
		ids          = []string{"4", "8", "15", "16", "23", "42"}
		expectedResp = `
			{"multicast_id": 123456789012345,
			 "success": 5,
			 "failure": 1,
			 "canonical_ids": 1,
			 "results": [
			   { "message_id": "10408" },
			   { "message_id": "10409" },
			   { "message_id": "11516" },
			   { "message_id": "11517" },
			   { "message_id": "12342", "registration_id": "32" },
			   { "error": "NotRegistered"}
			 ]
			}`
		multicastReply = `
			{ "multicast_id": 216,
			  "success": 3,
			  "failure": 3,
			  "canonical_ids": 1,
			  "results": [
			    { "message_id": "1:0408" },
			    { "error": "Unavailable" },
			    { "error": "InternalServerError" },
			    { "message_id": "1:1517" },
			    { "message_id": "1:2342", "registration_id": "32" },
			    { "error": "NotRegistered"}
			  ]
			}`
	)

	Context("initializing", func() {
		It("should init successfully", func() {
			c := newHTTPClient("key", true)
			Expect(c).To(BeAssignableToTypeOf(&fcmHTTP{}))
			gc := c.(*fcmHTTP)
			Expect(gc.apiKey).To(Equal("key"))
			Expect(gc.httpClient).NotTo(BeNil())
		})
	})

	Context("infrastructure", func() {
		It("should transform recipient to an array of strings", func() {
			singleTargetMessage := HTTPMessage{To: "recipient"}
			targets, err := messageTargetAsStringsArray(singleTargetMessage)
			Expect(err).NotTo(HaveOccurred())
			Expect(targets).To(Equal([]string{"recipient"}))

			multipleTargetMessage := HTTPMessage{RegistrationIDs: ids}
			targets, err = messageTargetAsStringsArray(multipleTargetMessage)
			Expect(err).NotTo(HaveOccurred())
			Expect(targets).To(Equal(ids))
		})

		It("should check http results", func() {
			response := &HTTPResponse{}
			json.Unmarshal([]byte(multicastReply), &response)
			resultsState := &multicastResultsState{}
			doRetry, toRetry, err := checkResults(response.Results, ids, *resultsState)
			Expect(doRetry).To(BeTrue())
			Expect(err).NotTo(HaveOccurred())
			Expect(toRetry).To(Equal([]string{"8", "15"}))
			expectedResultState := &multicastResultsState{
				"4":  &HTTPResult{MessageID: "1:0408"},
				"8":  &HTTPResult{Error: "Unavailable"},
				"15": &HTTPResult{Error: "InternalServerError"},
				"16": &HTTPResult{MessageID: "1:1517"},
				"23": &HTTPResult{MessageID: "1:2342", RegistrationID: "32"},
				"42": &HTTPResult{Error: "NotRegistered"},
			}
			Expect(resultsState).To(Equal(expectedResultState))
		})

		It("should build multicast response", func() {
			response := &HTTPResponse{}
			json.Unmarshal([]byte(multicastReply), &response)
			resultsState := multicastResultsState{
				"4":  &HTTPResult{MessageID: "1:0408"},
				"8":  &HTTPResult{Error: "Unavailable"},
				"15": &HTTPResult{Error: "InternalServerError"},
				"16": &HTTPResult{MessageID: "1:1517"},
				"23": &HTTPResult{MessageID: "1:2342", RegistrationID: "32"},
				"42": &HTTPResult{Error: "NotRegistered"},
			}
			resp := buildRespForMulticast(ids, resultsState, 216)
			Expect(resp).To(Equal(response))
		})

		It("should parse retryAfter correctly", func() {
			Expect(parseRetryAfter("10")).To(Equal(time.Second * 10))
			Expect(parseRetryAfter("Fri, 31 Dec 2030 23:59:59 GMT")).NotTo(BeZero())
			Expect(parseRetryAfter("Fri, 31 Dec 2000 23:59:59 GMT")).To(BeZero())
		})
	})

	Context("sending", func() {
		var (
			server     *httptest.Server
			c          httpC
			authHeader string
		)

		BeforeEach(func() {
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "application/json")
				w.Header().Set(http.CanonicalHeaderKey("Retry-After"), "10")
				w.WriteHeader(200)
				fmt.Fprintln(w, expectedResp)
			}))
			transport := &http.Transport{
				Proxy: func(req *http.Request) (*url.URL, error) {
					authHeader = req.Header.Get(http.CanonicalHeaderKey("Authorization"))
					return url.Parse(server.URL)
				},
			}
			c = &fcmHTTP{
				FCMURL:     server.URL,
				apiKey:     "apiKey",
				httpClient: &http.Client{Transport: transport},
			}
		})

		AfterEach(func() {
			server.Close()
		})

		It("should send successfully", func() {
			m := HTTPMessage{RegistrationIDs: ids}
			resp, err := c.Send(m)
			Expect(err).NotTo(HaveOccurred())
			expResp := HTTPResponse{}
			json.Unmarshal([]byte(expectedResp), &expResp)
			Expect(authHeader).To(Equal("key=apiKey"))
			Expect(resp).To(Equal(&expResp))
		})
	})
})
