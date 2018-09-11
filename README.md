GCM Library for Go [![GoDoc][godoc image]][godoc] [![Build Status][travis image]][travis] [![Coverage Status][codecov image]][codecov] [![Go Report Card][goreportcard image]][goreportcard]
--

Provides the following functionality for Google Cloud Messaging:

1. Sending messages via HTTP or XMPP.
2. Receiving messages from XMPP.

Documentation: [see godoc][godoc]

## Installation

    $ go get github.com/lilichen/go-fcm

## Status

This is a rework of [go-gcm library](https://github.com/google/go-gcm). It has the following improvements:
* code refactored, HTTP and XMPP clients separated
* monitors XMPP connection with pings, redials when ping fails
* handles CONNECTION_DRAINING properly
* graceful close
* exponential backoff when sending a message
* circuit breaker for each provider
* improved logging with [logrus](https://github.com/Sirupsen/logrus)
* [ginkgo](https://onsi.github.io/ginkgo/) tests
* various govet/golint fixes
* automatic builds with [Travis][travis] and coverage with [Codecov][codecov] 
* Replace GCM with FCM protocol

This library is in Beta. We will make an effort to support the library, but we reserve the right to make incompatible changes when necessary.

## Feedback

Please read CONTRIBUTING and raise issues here in Github.

## Limitations

Note that [GCM limitations][gcm limitations] are not enforced by the library. Instead, all valid requests are sent to the server and a corresponding error is returned (Message Too Big, Device Message Rate Exceeded, etc).

[godoc]: https://godoc.org/github.com/kikinteractive/go-gcm
[godoc image]: https://godoc.org/github.com/kikinteractive/go-gcm?status.svg

[travis image]: https://travis-ci.org/kikinteractive/go-gcm.svg
[travis]: https://travis-ci.org/kikinteractive/go-gcm

[codecov image]: https://codecov.io/gh/kikinteractive/go-gcm/branch/master/graph/badge.svg
[codecov]: https://codecov.io/gh/kikinteractive/go-gcm

[tag shield image]: https://img.shields.io/github/tag/kikinteractive/go-gcm.svg?maxAge=2592000

[goreportcard]: https://goreportcard.com/report/github.com/kikinteractive/go-gcm
[goreportcard image]: https://goreportcard.com/badge/github.com/kikinteractive/go-gcm

[gcm limitations]: https://gist.github.com/mnemonicflow/d08af1c1ea2f54f8667f
