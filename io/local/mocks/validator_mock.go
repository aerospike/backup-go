// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// Mockvalidator is an autogenerated mock type for the validator type
type Mockvalidator struct {
	mock.Mock
}

type Mockvalidator_Expecter struct {
	mock *mock.Mock
}

func (_m *Mockvalidator) EXPECT() *Mockvalidator_Expecter {
	return &Mockvalidator_Expecter{mock: &_m.Mock}
}

// Run provides a mock function with given fields: fileName
func (_m *Mockvalidator) Run(fileName string) error {
	ret := _m.Called(fileName)

	if len(ret) == 0 {
		panic("no return value specified for Run")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(fileName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Mockvalidator_Run_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Run'
type Mockvalidator_Run_Call struct {
	*mock.Call
}

// Run is a helper method to define mock.On call
//   - fileName string
func (_e *Mockvalidator_Expecter) Run(fileName interface{}) *Mockvalidator_Run_Call {
	return &Mockvalidator_Run_Call{Call: _e.mock.On("Run", fileName)}
}

func (_c *Mockvalidator_Run_Call) Run(run func(fileName string)) *Mockvalidator_Run_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *Mockvalidator_Run_Call) Return(_a0 error) *Mockvalidator_Run_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *Mockvalidator_Run_Call) RunAndReturn(run func(string) error) *Mockvalidator_Run_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockvalidator creates a new instance of Mockvalidator. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockvalidator(t interface {
	mock.TestingT
	Cleanup(func())
}) *Mockvalidator {
	mock := &Mockvalidator{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
