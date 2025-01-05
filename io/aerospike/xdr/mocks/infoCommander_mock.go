// Code generated by mockery v2.45.0. DO NOT EDIT.

package mocks

import (
	asinfo "github.com/aerospike/backup-go/internal/asinfo"
	mock "github.com/stretchr/testify/mock"
)

// MockinfoCommander is an autogenerated mock type for the infoCommander type
type MockinfoCommander struct {
	mock.Mock
}

type MockinfoCommander_Expecter struct {
	mock *mock.Mock
}

func (_m *MockinfoCommander) EXPECT() *MockinfoCommander_Expecter {
	return &MockinfoCommander_Expecter{mock: &_m.Mock}
}

// GetStats provides a mock function with given fields: dc, namespace
func (_m *MockinfoCommander) GetStats(dc string, namespace string) (asinfo.Stats, error) {
	ret := _m.Called(dc, namespace)

	if len(ret) == 0 {
		panic("no return value specified for GetStats")
	}

	var r0 asinfo.Stats
	var r1 error
	if rf, ok := ret.Get(0).(func(string, string) (asinfo.Stats, error)); ok {
		return rf(dc, namespace)
	}
	if rf, ok := ret.Get(0).(func(string, string) asinfo.Stats); ok {
		r0 = rf(dc, namespace)
	} else {
		r0 = ret.Get(0).(asinfo.Stats)
	}

	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(dc, namespace)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockinfoCommander_GetStats_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetStats'
type MockinfoCommander_GetStats_Call struct {
	*mock.Call
}

// GetStats is a helper method to define mock.On call
//   - dc string
//   - namespace string
func (_e *MockinfoCommander_Expecter) GetStats(dc interface{}, namespace interface{}) *MockinfoCommander_GetStats_Call {
	return &MockinfoCommander_GetStats_Call{Call: _e.mock.On("GetStats", dc, namespace)}
}

func (_c *MockinfoCommander_GetStats_Call) Run(run func(dc string, namespace string)) *MockinfoCommander_GetStats_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string))
	})
	return _c
}

func (_c *MockinfoCommander_GetStats_Call) Return(_a0 asinfo.Stats, _a1 error) *MockinfoCommander_GetStats_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockinfoCommander_GetStats_Call) RunAndReturn(run func(string, string) (asinfo.Stats, error)) *MockinfoCommander_GetStats_Call {
	_c.Call.Return(run)
	return _c
}

// StartXDR provides a mock function with given fields: dc, hostPort, namespace, rewind
func (_m *MockinfoCommander) StartXDR(dc string, hostPort string, namespace string, rewind string) error {
	ret := _m.Called(dc, hostPort, namespace, rewind)

	if len(ret) == 0 {
		panic("no return value specified for StartXDR")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, string, string) error); ok {
		r0 = rf(dc, hostPort, namespace, rewind)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockinfoCommander_StartXDR_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StartXDR'
type MockinfoCommander_StartXDR_Call struct {
	*mock.Call
}

// StartXDR is a helper method to define mock.On call
//   - dc string
//   - hostPort string
//   - namespace string
//   - rewind string
func (_e *MockinfoCommander_Expecter) StartXDR(dc interface{}, hostPort interface{}, namespace interface{}, rewind interface{}) *MockinfoCommander_StartXDR_Call {
	return &MockinfoCommander_StartXDR_Call{Call: _e.mock.On("StartXDR", dc, hostPort, namespace, rewind)}
}

func (_c *MockinfoCommander_StartXDR_Call) Run(run func(dc string, hostPort string, namespace string, rewind string)) *MockinfoCommander_StartXDR_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(string), args[3].(string))
	})
	return _c
}

func (_c *MockinfoCommander_StartXDR_Call) Return(_a0 error) *MockinfoCommander_StartXDR_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockinfoCommander_StartXDR_Call) RunAndReturn(run func(string, string, string, string) error) *MockinfoCommander_StartXDR_Call {
	_c.Call.Return(run)
	return _c
}

// StopXDR provides a mock function with given fields: dc, hostPort, namespace
func (_m *MockinfoCommander) StopXDR(dc string, hostPort string, namespace string) error {
	ret := _m.Called(dc, hostPort, namespace)

	if len(ret) == 0 {
		panic("no return value specified for StopXDR")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, string) error); ok {
		r0 = rf(dc, hostPort, namespace)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockinfoCommander_StopXDR_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StopXDR'
type MockinfoCommander_StopXDR_Call struct {
	*mock.Call
}

// StopXDR is a helper method to define mock.On call
//   - dc string
//   - hostPort string
//   - namespace string
func (_e *MockinfoCommander_Expecter) StopXDR(dc interface{}, hostPort interface{}, namespace interface{}) *MockinfoCommander_StopXDR_Call {
	return &MockinfoCommander_StopXDR_Call{Call: _e.mock.On("StopXDR", dc, hostPort, namespace)}
}

func (_c *MockinfoCommander_StopXDR_Call) Run(run func(dc string, hostPort string, namespace string)) *MockinfoCommander_StopXDR_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(string))
	})
	return _c
}

func (_c *MockinfoCommander_StopXDR_Call) Return(_a0 error) *MockinfoCommander_StopXDR_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockinfoCommander_StopXDR_Call) RunAndReturn(run func(string, string, string) error) *MockinfoCommander_StopXDR_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockinfoCommander creates a new instance of MockinfoCommander. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockinfoCommander(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockinfoCommander {
	mock := &MockinfoCommander{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}