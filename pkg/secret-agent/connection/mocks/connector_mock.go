// Code generated by mockery v2.42.1. DO NOT EDIT.

package mocks

import (
	time "time"

	mock "github.com/stretchr/testify/mock"
)

// Mockconnector is an autogenerated mock type for the connector type
type Mockconnector struct {
	mock.Mock
}

type Mockconnector_Expecter struct {
	mock *mock.Mock
}

func (_m *Mockconnector) EXPECT() *Mockconnector_Expecter {
	return &Mockconnector_Expecter{mock: &_m.Mock}
}

// Read provides a mock function with given fields: b
func (_m *Mockconnector) Read(b []byte) (int, error) {
	ret := _m.Called(b)

	if len(ret) == 0 {
		panic("no return value specified for Read")
	}

	var r0 int
	var r1 error
	if rf, ok := ret.Get(0).(func([]byte) (int, error)); ok {
		return rf(b)
	}
	if rf, ok := ret.Get(0).(func([]byte) int); ok {
		r0 = rf(b)
	} else {
		r0 = ret.Get(0).(int)
	}

	if rf, ok := ret.Get(1).(func([]byte) error); ok {
		r1 = rf(b)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Mockconnector_Read_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Read'
type Mockconnector_Read_Call struct {
	*mock.Call
}

// Read is a helper method to define mock.On call
//   - b []byte
func (_e *Mockconnector_Expecter) Read(b interface{}) *Mockconnector_Read_Call {
	return &Mockconnector_Read_Call{Call: _e.mock.On("Read", b)}
}

func (_c *Mockconnector_Read_Call) Run(run func(b []byte)) *Mockconnector_Read_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]byte))
	})
	return _c
}

func (_c *Mockconnector_Read_Call) Return(n int, err error) *Mockconnector_Read_Call {
	_c.Call.Return(n, err)
	return _c
}

func (_c *Mockconnector_Read_Call) RunAndReturn(run func([]byte) (int, error)) *Mockconnector_Read_Call {
	_c.Call.Return(run)
	return _c
}

// SetReadDeadline provides a mock function with given fields: t
func (_m *Mockconnector) SetReadDeadline(t time.Time) error {
	ret := _m.Called(t)

	if len(ret) == 0 {
		panic("no return value specified for SetReadDeadline")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(time.Time) error); ok {
		r0 = rf(t)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Mockconnector_SetReadDeadline_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetReadDeadline'
type Mockconnector_SetReadDeadline_Call struct {
	*mock.Call
}

// SetReadDeadline is a helper method to define mock.On call
//   - t time.Time
func (_e *Mockconnector_Expecter) SetReadDeadline(t interface{}) *Mockconnector_SetReadDeadline_Call {
	return &Mockconnector_SetReadDeadline_Call{Call: _e.mock.On("SetReadDeadline", t)}
}

func (_c *Mockconnector_SetReadDeadline_Call) Run(run func(t time.Time)) *Mockconnector_SetReadDeadline_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(time.Time))
	})
	return _c
}

func (_c *Mockconnector_SetReadDeadline_Call) Return(_a0 error) *Mockconnector_SetReadDeadline_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *Mockconnector_SetReadDeadline_Call) RunAndReturn(run func(time.Time) error) *Mockconnector_SetReadDeadline_Call {
	_c.Call.Return(run)
	return _c
}

// SetWriteDeadline provides a mock function with given fields: t
func (_m *Mockconnector) SetWriteDeadline(t time.Time) error {
	ret := _m.Called(t)

	if len(ret) == 0 {
		panic("no return value specified for SetWriteDeadline")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(time.Time) error); ok {
		r0 = rf(t)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Mockconnector_SetWriteDeadline_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetWriteDeadline'
type Mockconnector_SetWriteDeadline_Call struct {
	*mock.Call
}

// SetWriteDeadline is a helper method to define mock.On call
//   - t time.Time
func (_e *Mockconnector_Expecter) SetWriteDeadline(t interface{}) *Mockconnector_SetWriteDeadline_Call {
	return &Mockconnector_SetWriteDeadline_Call{Call: _e.mock.On("SetWriteDeadline", t)}
}

func (_c *Mockconnector_SetWriteDeadline_Call) Run(run func(t time.Time)) *Mockconnector_SetWriteDeadline_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(time.Time))
	})
	return _c
}

func (_c *Mockconnector_SetWriteDeadline_Call) Return(_a0 error) *Mockconnector_SetWriteDeadline_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *Mockconnector_SetWriteDeadline_Call) RunAndReturn(run func(time.Time) error) *Mockconnector_SetWriteDeadline_Call {
	_c.Call.Return(run)
	return _c
}

// Write provides a mock function with given fields: b
func (_m *Mockconnector) Write(b []byte) (int, error) {
	ret := _m.Called(b)

	if len(ret) == 0 {
		panic("no return value specified for Write")
	}

	var r0 int
	var r1 error
	if rf, ok := ret.Get(0).(func([]byte) (int, error)); ok {
		return rf(b)
	}
	if rf, ok := ret.Get(0).(func([]byte) int); ok {
		r0 = rf(b)
	} else {
		r0 = ret.Get(0).(int)
	}

	if rf, ok := ret.Get(1).(func([]byte) error); ok {
		r1 = rf(b)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Mockconnector_Write_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Write'
type Mockconnector_Write_Call struct {
	*mock.Call
}

// Write is a helper method to define mock.On call
//   - b []byte
func (_e *Mockconnector_Expecter) Write(b interface{}) *Mockconnector_Write_Call {
	return &Mockconnector_Write_Call{Call: _e.mock.On("Write", b)}
}

func (_c *Mockconnector_Write_Call) Run(run func(b []byte)) *Mockconnector_Write_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]byte))
	})
	return _c
}

func (_c *Mockconnector_Write_Call) Return(n int, err error) *Mockconnector_Write_Call {
	_c.Call.Return(n, err)
	return _c
}

func (_c *Mockconnector_Write_Call) RunAndReturn(run func([]byte) (int, error)) *Mockconnector_Write_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockconnector creates a new instance of Mockconnector. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockconnector(t interface {
	mock.TestingT
	Cleanup(func())
}) *Mockconnector {
	mock := &Mockconnector{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
