// Code generated by mockery v2.45.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// MockbinTypesInt is an autogenerated mock type for the binTypesInt type
type MockbinTypesInt struct {
	mock.Mock
}

type MockbinTypesInt_Expecter struct {
	mock *mock.Mock
}

func (_m *MockbinTypesInt) EXPECT() *MockbinTypesInt_Expecter {
	return &MockbinTypesInt_Expecter{mock: &_m.Mock}
}

// NewMockbinTypesInt creates a new instance of MockbinTypesInt. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockbinTypesInt(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockbinTypesInt {
	mock := &MockbinTypesInt{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
