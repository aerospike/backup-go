// Code generated by mockery v2.41.0. DO NOT EDIT.

package mocks

import (
	models "github.com/aerospike/aerospike-tools-backup-lib/models"
	mock "github.com/stretchr/testify/mock"
)

// SIndexGetter is an autogenerated mock type for the SIndexGetter type
type SIndexGetter struct {
	mock.Mock
}

type SIndexGetter_Expecter struct {
	mock *mock.Mock
}

func (_m *SIndexGetter) EXPECT() *SIndexGetter_Expecter {
	return &SIndexGetter_Expecter{mock: &_m.Mock}
}

// GetSIndexes provides a mock function with given fields: namespace
func (_m *SIndexGetter) GetSIndexes(namespace string) ([]*models.SIndex, error) {
	ret := _m.Called(namespace)

	if len(ret) == 0 {
		panic("no return value specified for GetSIndexes")
	}

	var r0 []*models.SIndex
	var r1 error
	if rf, ok := ret.Get(0).(func(string) ([]*models.SIndex, error)); ok {
		return rf(namespace)
	}
	if rf, ok := ret.Get(0).(func(string) []*models.SIndex); ok {
		r0 = rf(namespace)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*models.SIndex)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(namespace)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SIndexGetter_GetSIndexes_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSIndexes'
type SIndexGetter_GetSIndexes_Call struct {
	*mock.Call
}

// GetSIndexes is a helper method to define mock.On call
//   - namespace string
func (_e *SIndexGetter_Expecter) GetSIndexes(namespace interface{}) *SIndexGetter_GetSIndexes_Call {
	return &SIndexGetter_GetSIndexes_Call{Call: _e.mock.On("GetSIndexes", namespace)}
}

func (_c *SIndexGetter_GetSIndexes_Call) Run(run func(namespace string)) *SIndexGetter_GetSIndexes_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *SIndexGetter_GetSIndexes_Call) Return(_a0 []*models.SIndex, _a1 error) *SIndexGetter_GetSIndexes_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *SIndexGetter_GetSIndexes_Call) RunAndReturn(run func(string) ([]*models.SIndex, error)) *SIndexGetter_GetSIndexes_Call {
	_c.Call.Return(run)
	return _c
}

// NewSIndexGetter creates a new instance of SIndexGetter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewSIndexGetter(t interface {
	mock.TestingT
	Cleanup(func())
}) *SIndexGetter {
	mock := &SIndexGetter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
