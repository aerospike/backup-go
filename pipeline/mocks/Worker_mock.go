// Code generated by mockery v2.45.0. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// MockWorker is an autogenerated mock type for the Worker type
type MockWorker[T interface{}] struct {
	mock.Mock
}

type MockWorker_Expecter[T interface{}] struct {
	mock *mock.Mock
}

func (_m *MockWorker[T]) EXPECT() *MockWorker_Expecter[T] {
	return &MockWorker_Expecter[T]{mock: &_m.Mock}
}

// Run provides a mock function with given fields: _a0
func (_m *MockWorker[T]) Run(_a0 context.Context) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for Run")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockWorker_Run_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Run'
type MockWorker_Run_Call[T interface{}] struct {
	*mock.Call
}

// Run is a helper method to define mock.On call
//   - _a0 context.Context
func (_e *MockWorker_Expecter[T]) Run(_a0 interface{}) *MockWorker_Run_Call[T] {
	return &MockWorker_Run_Call[T]{Call: _e.mock.On("Run", _a0)}
}

func (_c *MockWorker_Run_Call[T]) Run(run func(_a0 context.Context)) *MockWorker_Run_Call[T] {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *MockWorker_Run_Call[T]) Return(_a0 error) *MockWorker_Run_Call[T] {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockWorker_Run_Call[T]) RunAndReturn(run func(context.Context) error) *MockWorker_Run_Call[T] {
	_c.Call.Return(run)
	return _c
}

// SetReceiveChan provides a mock function with given fields: _a0
func (_m *MockWorker[T]) SetReceiveChan(_a0 <-chan T) {
	_m.Called(_a0)
}

// MockWorker_SetReceiveChan_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetReceiveChan'
type MockWorker_SetReceiveChan_Call[T interface{}] struct {
	*mock.Call
}

// SetReceiveChan is a helper method to define mock.On call
//   - _a0 <-chan T
func (_e *MockWorker_Expecter[T]) SetReceiveChan(_a0 interface{}) *MockWorker_SetReceiveChan_Call[T] {
	return &MockWorker_SetReceiveChan_Call[T]{Call: _e.mock.On("SetReceiveChan", _a0)}
}

func (_c *MockWorker_SetReceiveChan_Call[T]) Run(run func(_a0 <-chan T)) *MockWorker_SetReceiveChan_Call[T] {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(<-chan T))
	})
	return _c
}

func (_c *MockWorker_SetReceiveChan_Call[T]) Return() *MockWorker_SetReceiveChan_Call[T] {
	_c.Call.Return()
	return _c
}

func (_c *MockWorker_SetReceiveChan_Call[T]) RunAndReturn(run func(<-chan T)) *MockWorker_SetReceiveChan_Call[T] {
	_c.Call.Return(run)
	return _c
}

// SetSendChan provides a mock function with given fields: _a0
func (_m *MockWorker[T]) SetSendChan(_a0 chan<- T) {
	_m.Called(_a0)
}

// MockWorker_SetSendChan_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetSendChan'
type MockWorker_SetSendChan_Call[T interface{}] struct {
	*mock.Call
}

// SetSendChan is a helper method to define mock.On call
//   - _a0 chan<- T
func (_e *MockWorker_Expecter[T]) SetSendChan(_a0 interface{}) *MockWorker_SetSendChan_Call[T] {
	return &MockWorker_SetSendChan_Call[T]{Call: _e.mock.On("SetSendChan", _a0)}
}

func (_c *MockWorker_SetSendChan_Call[T]) Run(run func(_a0 chan<- T)) *MockWorker_SetSendChan_Call[T] {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(chan<- T))
	})
	return _c
}

func (_c *MockWorker_SetSendChan_Call[T]) Return() *MockWorker_SetSendChan_Call[T] {
	_c.Call.Return()
	return _c
}

func (_c *MockWorker_SetSendChan_Call[T]) RunAndReturn(run func(chan<- T)) *MockWorker_SetSendChan_Call[T] {
	_c.Call.Return(run)
	return _c
}

// NewMockWorker creates a new instance of MockWorker. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockWorker[T interface{}](t interface {
	mock.TestingT
	Cleanup(func())
}) *MockWorker[T] {
	mock := &MockWorker[T]{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
