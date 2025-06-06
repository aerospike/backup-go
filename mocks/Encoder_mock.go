// Code generated by mockery; DO NOT EDIT.
// github.com/vektra/mockery
// template: testify

package mocks

import (
	"github.com/aerospike/backup-go/models"
	mock "github.com/stretchr/testify/mock"
)

// NewMockEncoder creates a new instance of MockEncoder. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockEncoder[T models.TokenConstraint](t interface {
	mock.TestingT
	Cleanup(func())
}) *MockEncoder[T] {
	mock := &MockEncoder[T]{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// MockEncoder is an autogenerated mock type for the Encoder type
type MockEncoder[T models.TokenConstraint] struct {
	mock.Mock
}

type MockEncoder_Expecter[T models.TokenConstraint] struct {
	mock *mock.Mock
}

func (_m *MockEncoder[T]) EXPECT() *MockEncoder_Expecter[T] {
	return &MockEncoder_Expecter[T]{mock: &_m.Mock}
}

// EncodeToken provides a mock function for the type MockEncoder
func (_mock *MockEncoder[T]) EncodeToken(v T) ([]byte, error) {
	ret := _mock.Called(v)

	if len(ret) == 0 {
		panic("no return value specified for EncodeToken")
	}

	var r0 []byte
	var r1 error
	if returnFunc, ok := ret.Get(0).(func(T) ([]byte, error)); ok {
		return returnFunc(v)
	}
	if returnFunc, ok := ret.Get(0).(func(T) []byte); ok {
		r0 = returnFunc(v)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}
	if returnFunc, ok := ret.Get(1).(func(T) error); ok {
		r1 = returnFunc(v)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// MockEncoder_EncodeToken_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'EncodeToken'
type MockEncoder_EncodeToken_Call[T models.TokenConstraint] struct {
	*mock.Call
}

// EncodeToken is a helper method to define mock.On call
//   - v
func (_e *MockEncoder_Expecter[T]) EncodeToken(v interface{}) *MockEncoder_EncodeToken_Call[T] {
	return &MockEncoder_EncodeToken_Call[T]{Call: _e.mock.On("EncodeToken", v)}
}

func (_c *MockEncoder_EncodeToken_Call[T]) Run(run func(v T)) *MockEncoder_EncodeToken_Call[T] {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(T))
	})
	return _c
}

func (_c *MockEncoder_EncodeToken_Call[T]) Return(bytes []byte, err error) *MockEncoder_EncodeToken_Call[T] {
	_c.Call.Return(bytes, err)
	return _c
}

func (_c *MockEncoder_EncodeToken_Call[T]) RunAndReturn(run func(v T) ([]byte, error)) *MockEncoder_EncodeToken_Call[T] {
	_c.Call.Return(run)
	return _c
}

// GenerateFilename provides a mock function for the type MockEncoder
func (_mock *MockEncoder[T]) GenerateFilename(prefix string, suffix string) string {
	ret := _mock.Called(prefix, suffix)

	if len(ret) == 0 {
		panic("no return value specified for GenerateFilename")
	}

	var r0 string
	if returnFunc, ok := ret.Get(0).(func(string, string) string); ok {
		r0 = returnFunc(prefix, suffix)
	} else {
		r0 = ret.Get(0).(string)
	}
	return r0
}

// MockEncoder_GenerateFilename_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GenerateFilename'
type MockEncoder_GenerateFilename_Call[T models.TokenConstraint] struct {
	*mock.Call
}

// GenerateFilename is a helper method to define mock.On call
//   - prefix
//   - suffix
func (_e *MockEncoder_Expecter[T]) GenerateFilename(prefix interface{}, suffix interface{}) *MockEncoder_GenerateFilename_Call[T] {
	return &MockEncoder_GenerateFilename_Call[T]{Call: _e.mock.On("GenerateFilename", prefix, suffix)}
}

func (_c *MockEncoder_GenerateFilename_Call[T]) Run(run func(prefix string, suffix string)) *MockEncoder_GenerateFilename_Call[T] {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string))
	})
	return _c
}

func (_c *MockEncoder_GenerateFilename_Call[T]) Return(s string) *MockEncoder_GenerateFilename_Call[T] {
	_c.Call.Return(s)
	return _c
}

func (_c *MockEncoder_GenerateFilename_Call[T]) RunAndReturn(run func(prefix string, suffix string) string) *MockEncoder_GenerateFilename_Call[T] {
	_c.Call.Return(run)
	return _c
}

// GetHeader provides a mock function for the type MockEncoder
func (_mock *MockEncoder[T]) GetHeader(v uint64) []byte {
	ret := _mock.Called(v)

	if len(ret) == 0 {
		panic("no return value specified for GetHeader")
	}

	var r0 []byte
	if returnFunc, ok := ret.Get(0).(func(uint64) []byte); ok {
		r0 = returnFunc(v)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}
	return r0
}

// MockEncoder_GetHeader_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetHeader'
type MockEncoder_GetHeader_Call[T models.TokenConstraint] struct {
	*mock.Call
}

// GetHeader is a helper method to define mock.On call
//   - v
func (_e *MockEncoder_Expecter[T]) GetHeader(v interface{}) *MockEncoder_GetHeader_Call[T] {
	return &MockEncoder_GetHeader_Call[T]{Call: _e.mock.On("GetHeader", v)}
}

func (_c *MockEncoder_GetHeader_Call[T]) Run(run func(v uint64)) *MockEncoder_GetHeader_Call[T] {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint64))
	})
	return _c
}

func (_c *MockEncoder_GetHeader_Call[T]) Return(bytes []byte) *MockEncoder_GetHeader_Call[T] {
	_c.Call.Return(bytes)
	return _c
}

func (_c *MockEncoder_GetHeader_Call[T]) RunAndReturn(run func(v uint64) []byte) *MockEncoder_GetHeader_Call[T] {
	_c.Call.Return(run)
	return _c
}
