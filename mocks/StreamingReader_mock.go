// Code generated by mockery v2.45.0. DO NOT EDIT.

package mocks

import (
	context "context"

	models "github.com/aerospike/backup-go/models"
	mock "github.com/stretchr/testify/mock"
)

// MockStreamingReader is an autogenerated mock type for the StreamingReader type
type MockStreamingReader struct {
	mock.Mock
}

type MockStreamingReader_Expecter struct {
	mock *mock.Mock
}

func (_m *MockStreamingReader) EXPECT() *MockStreamingReader_Expecter {
	return &MockStreamingReader_Expecter{mock: &_m.Mock}
}

// GetNumber provides a mock function with given fields:
func (_m *MockStreamingReader) GetNumber() int64 {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetNumber")
	}

	var r0 int64
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int64)
	}

	return r0
}

// MockStreamingReader_GetNumber_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetNumber'
type MockStreamingReader_GetNumber_Call struct {
	*mock.Call
}

// GetNumber is a helper method to define mock.On call
func (_e *MockStreamingReader_Expecter) GetNumber() *MockStreamingReader_GetNumber_Call {
	return &MockStreamingReader_GetNumber_Call{Call: _e.mock.On("GetNumber")}
}

func (_c *MockStreamingReader_GetNumber_Call) Run(run func()) *MockStreamingReader_GetNumber_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockStreamingReader_GetNumber_Call) Return(_a0 int64) *MockStreamingReader_GetNumber_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockStreamingReader_GetNumber_Call) RunAndReturn(run func() int64) *MockStreamingReader_GetNumber_Call {
	_c.Call.Return(run)
	return _c
}

// GetSize provides a mock function with given fields:
func (_m *MockStreamingReader) GetSize() int64 {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetSize")
	}

	var r0 int64
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int64)
	}

	return r0
}

// MockStreamingReader_GetSize_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSize'
type MockStreamingReader_GetSize_Call struct {
	*mock.Call
}

// GetSize is a helper method to define mock.On call
func (_e *MockStreamingReader_Expecter) GetSize() *MockStreamingReader_GetSize_Call {
	return &MockStreamingReader_GetSize_Call{Call: _e.mock.On("GetSize")}
}

func (_c *MockStreamingReader_GetSize_Call) Run(run func()) *MockStreamingReader_GetSize_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockStreamingReader_GetSize_Call) Return(_a0 int64) *MockStreamingReader_GetSize_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockStreamingReader_GetSize_Call) RunAndReturn(run func() int64) *MockStreamingReader_GetSize_Call {
	_c.Call.Return(run)
	return _c
}

// GetType provides a mock function with given fields:
func (_m *MockStreamingReader) GetType() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetType")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// MockStreamingReader_GetType_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetType'
type MockStreamingReader_GetType_Call struct {
	*mock.Call
}

// GetType is a helper method to define mock.On call
func (_e *MockStreamingReader_Expecter) GetType() *MockStreamingReader_GetType_Call {
	return &MockStreamingReader_GetType_Call{Call: _e.mock.On("GetType")}
}

func (_c *MockStreamingReader_GetType_Call) Run(run func()) *MockStreamingReader_GetType_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockStreamingReader_GetType_Call) Return(_a0 string) *MockStreamingReader_GetType_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockStreamingReader_GetType_Call) RunAndReturn(run func() string) *MockStreamingReader_GetType_Call {
	_c.Call.Return(run)
	return _c
}

// ListObjects provides a mock function with given fields: ctx, path
func (_m *MockStreamingReader) ListObjects(ctx context.Context, path string) ([]string, error) {
	ret := _m.Called(ctx, path)

	if len(ret) == 0 {
		panic("no return value specified for ListObjects")
	}

	var r0 []string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) ([]string, error)); ok {
		return rf(ctx, path)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) []string); ok {
		r0 = rf(ctx, path)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, path)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockStreamingReader_ListObjects_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListObjects'
type MockStreamingReader_ListObjects_Call struct {
	*mock.Call
}

// ListObjects is a helper method to define mock.On call
//   - ctx context.Context
//   - path string
func (_e *MockStreamingReader_Expecter) ListObjects(ctx interface{}, path interface{}) *MockStreamingReader_ListObjects_Call {
	return &MockStreamingReader_ListObjects_Call{Call: _e.mock.On("ListObjects", ctx, path)}
}

func (_c *MockStreamingReader_ListObjects_Call) Run(run func(ctx context.Context, path string)) *MockStreamingReader_ListObjects_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *MockStreamingReader_ListObjects_Call) Return(_a0 []string, _a1 error) *MockStreamingReader_ListObjects_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockStreamingReader_ListObjects_Call) RunAndReturn(run func(context.Context, string) ([]string, error)) *MockStreamingReader_ListObjects_Call {
	_c.Call.Return(run)
	return _c
}

// StreamFile provides a mock function with given fields: ctx, filename, readersCh, errorsCh
func (_m *MockStreamingReader) StreamFile(ctx context.Context, filename string, readersCh chan<- models.File, errorsCh chan<- error) {
	_m.Called(ctx, filename, readersCh, errorsCh)
}

// MockStreamingReader_StreamFile_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StreamFile'
type MockStreamingReader_StreamFile_Call struct {
	*mock.Call
}

// StreamFile is a helper method to define mock.On call
//   - ctx context.Context
//   - filename string
//   - readersCh chan<- models.File
//   - errorsCh chan<- error
func (_e *MockStreamingReader_Expecter) StreamFile(ctx interface{}, filename interface{}, readersCh interface{}, errorsCh interface{}) *MockStreamingReader_StreamFile_Call {
	return &MockStreamingReader_StreamFile_Call{Call: _e.mock.On("StreamFile", ctx, filename, readersCh, errorsCh)}
}

func (_c *MockStreamingReader_StreamFile_Call) Run(run func(ctx context.Context, filename string, readersCh chan<- models.File, errorsCh chan<- error)) *MockStreamingReader_StreamFile_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(chan<- models.File), args[3].(chan<- error))
	})
	return _c
}

func (_c *MockStreamingReader_StreamFile_Call) Return() *MockStreamingReader_StreamFile_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockStreamingReader_StreamFile_Call) RunAndReturn(run func(context.Context, string, chan<- models.File, chan<- error)) *MockStreamingReader_StreamFile_Call {
	_c.Call.Return(run)
	return _c
}

// StreamFiles provides a mock function with given fields: _a0, _a1, _a2
func (_m *MockStreamingReader) StreamFiles(_a0 context.Context, _a1 chan<- models.File, _a2 chan<- error) {
	_m.Called(_a0, _a1, _a2)
}

// MockStreamingReader_StreamFiles_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StreamFiles'
type MockStreamingReader_StreamFiles_Call struct {
	*mock.Call
}

// StreamFiles is a helper method to define mock.On call
//   - _a0 context.Context
//   - _a1 chan<- models.File
//   - _a2 chan<- error
func (_e *MockStreamingReader_Expecter) StreamFiles(_a0 interface{}, _a1 interface{}, _a2 interface{}) *MockStreamingReader_StreamFiles_Call {
	return &MockStreamingReader_StreamFiles_Call{Call: _e.mock.On("StreamFiles", _a0, _a1, _a2)}
}

func (_c *MockStreamingReader_StreamFiles_Call) Run(run func(_a0 context.Context, _a1 chan<- models.File, _a2 chan<- error)) *MockStreamingReader_StreamFiles_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(chan<- models.File), args[2].(chan<- error))
	})
	return _c
}

func (_c *MockStreamingReader_StreamFiles_Call) Return() *MockStreamingReader_StreamFiles_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockStreamingReader_StreamFiles_Call) RunAndReturn(run func(context.Context, chan<- models.File, chan<- error)) *MockStreamingReader_StreamFiles_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockStreamingReader creates a new instance of MockStreamingReader. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockStreamingReader(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockStreamingReader {
	mock := &MockStreamingReader{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
