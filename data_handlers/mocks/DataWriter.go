// Code generated by mockery v2.41.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// DataWriter is an autogenerated mock type for the DataWriter type
type DataWriter struct {
	mock.Mock
}

type DataWriter_Expecter struct {
	mock *mock.Mock
}

func (_m *DataWriter) EXPECT() *DataWriter_Expecter {
	return &DataWriter_Expecter{mock: &_m.Mock}
}

// Cancel provides a mock function with given fields:
func (_m *DataWriter) Cancel() {
	_m.Called()
}

// DataWriter_Cancel_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Cancel'
type DataWriter_Cancel_Call struct {
	*mock.Call
}

// Cancel is a helper method to define mock.On call
func (_e *DataWriter_Expecter) Cancel() *DataWriter_Cancel_Call {
	return &DataWriter_Cancel_Call{Call: _e.mock.On("Cancel")}
}

func (_c *DataWriter_Cancel_Call) Run(run func()) *DataWriter_Cancel_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *DataWriter_Cancel_Call) Return() *DataWriter_Cancel_Call {
	_c.Call.Return()
	return _c
}

func (_c *DataWriter_Cancel_Call) RunAndReturn(run func()) *DataWriter_Cancel_Call {
	_c.Call.Return(run)
	return _c
}

// Write provides a mock function with given fields: _a0
func (_m *DataWriter) Write(_a0 interface{}) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for Write")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DataWriter_Write_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Write'
type DataWriter_Write_Call struct {
	*mock.Call
}

// Write is a helper method to define mock.On call
//   - _a0 interface{}
func (_e *DataWriter_Expecter) Write(_a0 interface{}) *DataWriter_Write_Call {
	return &DataWriter_Write_Call{Call: _e.mock.On("Write", _a0)}
}

func (_c *DataWriter_Write_Call) Run(run func(_a0 interface{})) *DataWriter_Write_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(interface{}))
	})
	return _c
}

func (_c *DataWriter_Write_Call) Return(_a0 error) *DataWriter_Write_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *DataWriter_Write_Call) RunAndReturn(run func(interface{}) error) *DataWriter_Write_Call {
	_c.Call.Return(run)
	return _c
}

// NewDataWriter creates a new instance of DataWriter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewDataWriter(t interface {
	mock.TestingT
	Cleanup(func())
}) *DataWriter {
	mock := &DataWriter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
