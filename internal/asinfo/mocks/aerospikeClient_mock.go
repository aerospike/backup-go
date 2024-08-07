// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocks

import (
	aerospike "github.com/aerospike/aerospike-client-go/v7"

	mock "github.com/stretchr/testify/mock"
)

// MockaerospikeClient is an autogenerated mock type for the aerospikeClient type
type MockaerospikeClient struct {
	mock.Mock
}

type MockaerospikeClient_Expecter struct {
	mock *mock.Mock
}

func (_m *MockaerospikeClient) EXPECT() *MockaerospikeClient_Expecter {
	return &MockaerospikeClient_Expecter{mock: &_m.Mock}
}

// Cluster provides a mock function with given fields:
func (_m *MockaerospikeClient) Cluster() *aerospike.Cluster {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Cluster")
	}

	var r0 *aerospike.Cluster
	if rf, ok := ret.Get(0).(func() *aerospike.Cluster); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*aerospike.Cluster)
		}
	}

	return r0
}

// MockaerospikeClient_Cluster_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Cluster'
type MockaerospikeClient_Cluster_Call struct {
	*mock.Call
}

// Cluster is a helper method to define mock.On call
func (_e *MockaerospikeClient_Expecter) Cluster() *MockaerospikeClient_Cluster_Call {
	return &MockaerospikeClient_Cluster_Call{Call: _e.mock.On("Cluster")}
}

func (_c *MockaerospikeClient_Cluster_Call) Run(run func()) *MockaerospikeClient_Cluster_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockaerospikeClient_Cluster_Call) Return(_a0 *aerospike.Cluster) *MockaerospikeClient_Cluster_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockaerospikeClient_Cluster_Call) RunAndReturn(run func() *aerospike.Cluster) *MockaerospikeClient_Cluster_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockaerospikeClient creates a new instance of MockaerospikeClient. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockaerospikeClient(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockaerospikeClient {
	mock := &MockaerospikeClient{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}