// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocks

import (
	aerospike "github.com/aerospike/aerospike-client-go/v7"

	mock "github.com/stretchr/testify/mock"
)

// MockinfoGetter is an autogenerated mock type for the infoGetter type
type MockinfoGetter struct {
	mock.Mock
}

type MockinfoGetter_Expecter struct {
	mock *mock.Mock
}

func (_m *MockinfoGetter) EXPECT() *MockinfoGetter_Expecter {
	return &MockinfoGetter_Expecter{mock: &_m.Mock}
}

// RequestInfo provides a mock function with given fields: infoPolicy, commands
func (_m *MockinfoGetter) RequestInfo(infoPolicy *aerospike.InfoPolicy, commands ...string) (map[string]string, aerospike.Error) {
	_va := make([]interface{}, len(commands))
	for _i := range commands {
		_va[_i] = commands[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, infoPolicy)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for RequestInfo")
	}

	var r0 map[string]string
	var r1 aerospike.Error
	if rf, ok := ret.Get(0).(func(*aerospike.InfoPolicy, ...string) (map[string]string, aerospike.Error)); ok {
		return rf(infoPolicy, commands...)
	}
	if rf, ok := ret.Get(0).(func(*aerospike.InfoPolicy, ...string) map[string]string); ok {
		r0 = rf(infoPolicy, commands...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]string)
		}
	}

	if rf, ok := ret.Get(1).(func(*aerospike.InfoPolicy, ...string) aerospike.Error); ok {
		r1 = rf(infoPolicy, commands...)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(aerospike.Error)
		}
	}

	return r0, r1
}

// MockinfoGetter_RequestInfo_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RequestInfo'
type MockinfoGetter_RequestInfo_Call struct {
	*mock.Call
}

// RequestInfo is a helper method to define mock.On call
//   - infoPolicy *aerospike.InfoPolicy
//   - commands ...string
func (_e *MockinfoGetter_Expecter) RequestInfo(infoPolicy interface{}, commands ...interface{}) *MockinfoGetter_RequestInfo_Call {
	return &MockinfoGetter_RequestInfo_Call{Call: _e.mock.On("RequestInfo",
		append([]interface{}{infoPolicy}, commands...)...)}
}

func (_c *MockinfoGetter_RequestInfo_Call) Run(run func(infoPolicy *aerospike.InfoPolicy, commands ...string)) *MockinfoGetter_RequestInfo_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]string, len(args)-1)
		for i, a := range args[1:] {
			if a != nil {
				variadicArgs[i] = a.(string)
			}
		}
		run(args[0].(*aerospike.InfoPolicy), variadicArgs...)
	})
	return _c
}

func (_c *MockinfoGetter_RequestInfo_Call) Return(_a0 map[string]string, _a1 aerospike.Error) *MockinfoGetter_RequestInfo_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockinfoGetter_RequestInfo_Call) RunAndReturn(run func(*aerospike.InfoPolicy, ...string) (map[string]string, aerospike.Error)) *MockinfoGetter_RequestInfo_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockinfoGetter creates a new instance of MockinfoGetter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockinfoGetter(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockinfoGetter {
	mock := &MockinfoGetter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
