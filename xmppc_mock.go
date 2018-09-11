package fcm

import "github.com/stretchr/testify/mock"

import "time"

// xmppCMock is an autogenerated mock type for the xmppC type
type xmppCMock struct {
	mock.Mock
}

// Close provides a mock function with given fields: graceful
func (_m *xmppCMock) Close(graceful bool) error {
	ret := _m.Called(graceful)

	var r0 error
	if rf, ok := ret.Get(0).(func(bool) error); ok {
		r0 = rf(graceful)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// IsClosed provides a mock function with given fields:
func (_m *xmppCMock) IsClosed() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// Listen provides a mock function with given fields: h
func (_m *xmppCMock) Listen(h MessageHandler) error {
	ret := _m.Called(h)

	var r0 error
	if rf, ok := ret.Get(0).(func(MessageHandler) error); ok {
		r0 = rf(h)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Ping provides a mock function with given fields: timeout
func (_m *xmppCMock) Ping(timeout time.Duration) error {
	ret := _m.Called(timeout)

	var r0 error
	if rf, ok := ret.Get(0).(func(time.Duration) error); ok {
		r0 = rf(timeout)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Send provides a mock function with given fields: m
func (_m *xmppCMock) Send(m XMPPMessage) (string, int, error) {
	ret := _m.Called(m)

	var r0 string
	if rf, ok := ret.Get(0).(func(XMPPMessage) string); ok {
		r0 = rf(m)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 int
	if rf, ok := ret.Get(1).(func(XMPPMessage) int); ok {
		r1 = rf(m)
	} else {
		r1 = ret.Get(1).(int)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(XMPPMessage) error); ok {
		r2 = rf(m)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// ID provides a mock function with given fields:
func (_m *xmppCMock) ID() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// JID provides a mock function with given fields:
func (_m *xmppCMock) JID() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}
