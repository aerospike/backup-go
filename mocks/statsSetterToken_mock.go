// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// MockstatsSetterToken is an autogenerated mock type for the statsSetterToken type
type MockstatsSetterToken struct {
	mock.Mock
}

type MockstatsSetterToken_Expecter struct {
	mock *mock.Mock
}

func (_m *MockstatsSetterToken) EXPECT() *MockstatsSetterToken_Expecter {
	return &MockstatsSetterToken_Expecter{mock: &_m.Mock}
}

// AddSIndexes provides a mock function with given fields: _a0
func (_m *MockstatsSetterToken) AddSIndexes(_a0 uint32) {
	_m.Called(_a0)
}

// MockstatsSetterToken_AddSIndexes_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AddSIndexes'
type MockstatsSetterToken_AddSIndexes_Call struct {
	*mock.Call
}

// AddSIndexes is a helper method to define mock.On call
//   - _a0 uint32
func (_e *MockstatsSetterToken_Expecter) AddSIndexes(_a0 interface{}) *MockstatsSetterToken_AddSIndexes_Call {
	return &MockstatsSetterToken_AddSIndexes_Call{Call: _e.mock.On("AddSIndexes", _a0)}
}

func (_c *MockstatsSetterToken_AddSIndexes_Call) Run(run func(_a0 uint32)) *MockstatsSetterToken_AddSIndexes_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint32))
	})
	return _c
}

func (_c *MockstatsSetterToken_AddSIndexes_Call) Return() *MockstatsSetterToken_AddSIndexes_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockstatsSetterToken_AddSIndexes_Call) RunAndReturn(run func(uint32)) *MockstatsSetterToken_AddSIndexes_Call {
	_c.Call.Return(run)
	return _c
}

// AddUDFs provides a mock function with given fields: _a0
func (_m *MockstatsSetterToken) AddUDFs(_a0 uint32) {
	_m.Called(_a0)
}

// MockstatsSetterToken_AddUDFs_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AddUDFs'
type MockstatsSetterToken_AddUDFs_Call struct {
	*mock.Call
}

// AddUDFs is a helper method to define mock.On call
//   - _a0 uint32
func (_e *MockstatsSetterToken_Expecter) AddUDFs(_a0 interface{}) *MockstatsSetterToken_AddUDFs_Call {
	return &MockstatsSetterToken_AddUDFs_Call{Call: _e.mock.On("AddUDFs", _a0)}
}

func (_c *MockstatsSetterToken_AddUDFs_Call) Run(run func(_a0 uint32)) *MockstatsSetterToken_AddUDFs_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint32))
	})
	return _c
}

func (_c *MockstatsSetterToken_AddUDFs_Call) Return() *MockstatsSetterToken_AddUDFs_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockstatsSetterToken_AddUDFs_Call) RunAndReturn(run func(uint32)) *MockstatsSetterToken_AddUDFs_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockstatsSetterToken creates a new instance of MockstatsSetterToken. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockstatsSetterToken(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockstatsSetterToken {
	mock := &MockstatsSetterToken{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
