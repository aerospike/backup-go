// Code generated by mockery; DO NOT EDIT.
// github.com/vektra/mockery
// template: testify

package mocks

import (
	"context"

	"github.com/aerospike/backup-go/models"
	mock "github.com/stretchr/testify/mock"
)

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

// GetNumber provides a mock function for the type MockStreamingReader
func (_mock *MockStreamingReader) GetNumber() int64 {
	ret := _mock.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetNumber")
	}

	var r0 int64
	if returnFunc, ok := ret.Get(0).(func() int64); ok {
		r0 = returnFunc()
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

func (_c *MockStreamingReader_GetNumber_Call) Return(n int64) *MockStreamingReader_GetNumber_Call {
	_c.Call.Return(n)
	return _c
}

func (_c *MockStreamingReader_GetNumber_Call) RunAndReturn(run func() int64) *MockStreamingReader_GetNumber_Call {
	_c.Call.Return(run)
	return _c
}

// GetSize provides a mock function for the type MockStreamingReader
func (_mock *MockStreamingReader) GetSize() int64 {
	ret := _mock.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetSize")
	}

	var r0 int64
	if returnFunc, ok := ret.Get(0).(func() int64); ok {
		r0 = returnFunc()
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

func (_c *MockStreamingReader_GetSize_Call) Return(n int64) *MockStreamingReader_GetSize_Call {
	_c.Call.Return(n)
	return _c
}

func (_c *MockStreamingReader_GetSize_Call) RunAndReturn(run func() int64) *MockStreamingReader_GetSize_Call {
	_c.Call.Return(run)
	return _c
}

// GetType provides a mock function for the type MockStreamingReader
func (_mock *MockStreamingReader) GetType() string {
	ret := _mock.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetType")
	}

	var r0 string
	if returnFunc, ok := ret.Get(0).(func() string); ok {
		r0 = returnFunc()
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

func (_c *MockStreamingReader_GetType_Call) Return(s string) *MockStreamingReader_GetType_Call {
	_c.Call.Return(s)
	return _c
}

func (_c *MockStreamingReader_GetType_Call) RunAndReturn(run func() string) *MockStreamingReader_GetType_Call {
	_c.Call.Return(run)
	return _c
}

// ListObjects provides a mock function for the type MockStreamingReader
func (_mock *MockStreamingReader) ListObjects(ctx context.Context, path string) ([]string, error) {
	ret := _mock.Called(ctx, path)

	if len(ret) == 0 {
		panic("no return value specified for ListObjects")
	}

	var r0 []string
	var r1 error
	if returnFunc, ok := ret.Get(0).(func(context.Context, string) ([]string, error)); ok {
		return returnFunc(ctx, path)
	}
	if returnFunc, ok := ret.Get(0).(func(context.Context, string) []string); ok {
		r0 = returnFunc(ctx, path)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}
	if returnFunc, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = returnFunc(ctx, path)
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
//   - ctx
//   - path
func (_e *MockStreamingReader_Expecter) ListObjects(ctx interface{}, path interface{}) *MockStreamingReader_ListObjects_Call {
	return &MockStreamingReader_ListObjects_Call{Call: _e.mock.On("ListObjects", ctx, path)}
}

func (_c *MockStreamingReader_ListObjects_Call) Run(run func(ctx context.Context, path string)) *MockStreamingReader_ListObjects_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *MockStreamingReader_ListObjects_Call) Return(strings []string, err error) *MockStreamingReader_ListObjects_Call {
	_c.Call.Return(strings, err)
	return _c
}

func (_c *MockStreamingReader_ListObjects_Call) RunAndReturn(run func(ctx context.Context, path string) ([]string, error)) *MockStreamingReader_ListObjects_Call {
	_c.Call.Return(run)
	return _c
}

// StreamFile provides a mock function for the type MockStreamingReader
func (_mock *MockStreamingReader) StreamFile(ctx context.Context, filename string, readersCh chan<- models.File, errorsCh chan<- error) {
	_mock.Called(ctx, filename, readersCh, errorsCh)
	return
}

// MockStreamingReader_StreamFile_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StreamFile'
type MockStreamingReader_StreamFile_Call struct {
	*mock.Call
}

// StreamFile is a helper method to define mock.On call
//   - ctx
//   - filename
//   - readersCh
//   - errorsCh
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

func (_c *MockStreamingReader_StreamFile_Call) RunAndReturn(run func(ctx context.Context, filename string, readersCh chan<- models.File, errorsCh chan<- error)) *MockStreamingReader_StreamFile_Call {
	_c.Run(run)
	return _c
}

// StreamFiles provides a mock function for the type MockStreamingReader
func (_mock *MockStreamingReader) StreamFiles(context1 context.Context, fileCh chan<- models.File, errCh chan<- error) {
	_mock.Called(context1, fileCh, errCh)
	return
}

// MockStreamingReader_StreamFiles_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StreamFiles'
type MockStreamingReader_StreamFiles_Call struct {
	*mock.Call
}

// StreamFiles is a helper method to define mock.On call
//   - context1
//   - fileCh
//   - errCh
func (_e *MockStreamingReader_Expecter) StreamFiles(context1 interface{}, fileCh interface{}, errCh interface{}) *MockStreamingReader_StreamFiles_Call {
	return &MockStreamingReader_StreamFiles_Call{Call: _e.mock.On("StreamFiles", context1, fileCh, errCh)}
}

func (_c *MockStreamingReader_StreamFiles_Call) Run(run func(context1 context.Context, fileCh chan<- models.File, errCh chan<- error)) *MockStreamingReader_StreamFiles_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(chan<- models.File), args[2].(chan<- error))
	})
	return _c
}

func (_c *MockStreamingReader_StreamFiles_Call) Return() *MockStreamingReader_StreamFiles_Call {
	_c.Call.Return()
	return _c
}

func (_c *MockStreamingReader_StreamFiles_Call) RunAndReturn(run func(context1 context.Context, fileCh chan<- models.File, errCh chan<- error)) *MockStreamingReader_StreamFiles_Call {
	_c.Run(run)
	return _c
}
