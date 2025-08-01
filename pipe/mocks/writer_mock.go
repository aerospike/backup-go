// Code generated by mockery; DO NOT EDIT.
// github.com/vektra/mockery
// template: testify

package mocks

import (
	"context"

	"github.com/aerospike/backup-go/models"
	mock "github.com/stretchr/testify/mock"
)

// NewMockWriter creates a new instance of MockWriter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockWriter[T models.TokenConstraint](t interface {
	mock.TestingT
	Cleanup(func())
}) *MockWriter[T] {
	mock := &MockWriter[T]{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// MockWriter is an autogenerated mock type for the Writer type
type MockWriter[T models.TokenConstraint] struct {
	mock.Mock
}

type MockWriter_Expecter[T models.TokenConstraint] struct {
	mock *mock.Mock
}

func (_m *MockWriter[T]) EXPECT() *MockWriter_Expecter[T] {
	return &MockWriter_Expecter[T]{mock: &_m.Mock}
}

// Close provides a mock function for the type MockWriter
func (_mock *MockWriter[T]) Close() error {
	ret := _mock.Called()

	if len(ret) == 0 {
		panic("no return value specified for Close")
	}

	var r0 error
	if returnFunc, ok := ret.Get(0).(func() error); ok {
		r0 = returnFunc()
	} else {
		r0 = ret.Error(0)
	}
	return r0
}

// MockWriter_Close_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Close'
type MockWriter_Close_Call[T models.TokenConstraint] struct {
	*mock.Call
}

// Close is a helper method to define mock.On call
func (_e *MockWriter_Expecter[T]) Close() *MockWriter_Close_Call[T] {
	return &MockWriter_Close_Call[T]{Call: _e.mock.On("Close")}
}

func (_c *MockWriter_Close_Call[T]) Run(run func()) *MockWriter_Close_Call[T] {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockWriter_Close_Call[T]) Return(err error) *MockWriter_Close_Call[T] {
	_c.Call.Return(err)
	return _c
}

func (_c *MockWriter_Close_Call[T]) RunAndReturn(run func() error) *MockWriter_Close_Call[T] {
	_c.Call.Return(run)
	return _c
}

// Write provides a mock function for the type MockWriter
func (_mock *MockWriter[T]) Write(context1 context.Context, v T) (int, error) {
	ret := _mock.Called(context1, v)

	if len(ret) == 0 {
		panic("no return value specified for Write")
	}

	var r0 int
	var r1 error
	if returnFunc, ok := ret.Get(0).(func(context.Context, T) (int, error)); ok {
		return returnFunc(context1, v)
	}
	if returnFunc, ok := ret.Get(0).(func(context.Context, T) int); ok {
		r0 = returnFunc(context1, v)
	} else {
		r0 = ret.Get(0).(int)
	}
	if returnFunc, ok := ret.Get(1).(func(context.Context, T) error); ok {
		r1 = returnFunc(context1, v)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// MockWriter_Write_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Write'
type MockWriter_Write_Call[T models.TokenConstraint] struct {
	*mock.Call
}

// Write is a helper method to define mock.On call
//   - context1
//   - v
func (_e *MockWriter_Expecter[T]) Write(context1 interface{}, v interface{}) *MockWriter_Write_Call[T] {
	return &MockWriter_Write_Call[T]{Call: _e.mock.On("Write", context1, v)}
}

func (_c *MockWriter_Write_Call[T]) Run(run func(context1 context.Context, v T)) *MockWriter_Write_Call[T] {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(T))
	})
	return _c
}

func (_c *MockWriter_Write_Call[T]) Return(n int, err error) *MockWriter_Write_Call[T] {
	_c.Call.Return(n, err)
	return _c
}

func (_c *MockWriter_Write_Call[T]) RunAndReturn(run func(context1 context.Context, v T) (int, error)) *MockWriter_Write_Call[T] {
	_c.Call.Return(run)
	return _c
}
