// Code generated by mockery; DO NOT EDIT.
// github.com/vektra/mockery
// template: testify

package mocks

import (
	"github.com/aerospike/aerospike-client-go/v8"
	mock "github.com/stretchr/testify/mock"
)

// NewMockscanner creates a new instance of Mockscanner. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockscanner(t interface {
	mock.TestingT
	Cleanup(func())
}) *Mockscanner {
	mock := &Mockscanner{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// Mockscanner is an autogenerated mock type for the scanner type
type Mockscanner struct {
	mock.Mock
}

type Mockscanner_Expecter struct {
	mock *mock.Mock
}

func (_m *Mockscanner) EXPECT() *Mockscanner_Expecter {
	return &Mockscanner_Expecter{mock: &_m.Mock}
}

// ScanNode provides a mock function for the type Mockscanner
func (_mock *Mockscanner) ScanNode(scanPolicy *aerospike.ScanPolicy, node *aerospike.Node, namespace string, setName string, binNames ...string) (*aerospike.Recordset, aerospike.Error) {
	var tmpRet mock.Arguments
	if len(binNames) > 0 {
		tmpRet = _mock.Called(scanPolicy, node, namespace, setName, binNames)
	} else {
		tmpRet = _mock.Called(scanPolicy, node, namespace, setName)
	}
	ret := tmpRet

	if len(ret) == 0 {
		panic("no return value specified for ScanNode")
	}

	var r0 *aerospike.Recordset
	var r1 aerospike.Error
	if returnFunc, ok := ret.Get(0).(func(*aerospike.ScanPolicy, *aerospike.Node, string, string, ...string) (*aerospike.Recordset, aerospike.Error)); ok {
		return returnFunc(scanPolicy, node, namespace, setName, binNames...)
	}
	if returnFunc, ok := ret.Get(0).(func(*aerospike.ScanPolicy, *aerospike.Node, string, string, ...string) *aerospike.Recordset); ok {
		r0 = returnFunc(scanPolicy, node, namespace, setName, binNames...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*aerospike.Recordset)
		}
	}
	if returnFunc, ok := ret.Get(1).(func(*aerospike.ScanPolicy, *aerospike.Node, string, string, ...string) aerospike.Error); ok {
		r1 = returnFunc(scanPolicy, node, namespace, setName, binNames...)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(aerospike.Error)
		}
	}
	return r0, r1
}

// Mockscanner_ScanNode_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ScanNode'
type Mockscanner_ScanNode_Call struct {
	*mock.Call
}

// ScanNode is a helper method to define mock.On call
//   - scanPolicy
//   - node
//   - namespace
//   - setName
//   - binNames
func (_e *Mockscanner_Expecter) ScanNode(scanPolicy interface{}, node interface{}, namespace interface{}, setName interface{}, binNames ...interface{}) *Mockscanner_ScanNode_Call {
	return &Mockscanner_ScanNode_Call{Call: _e.mock.On("ScanNode",
		append([]interface{}{scanPolicy, node, namespace, setName}, binNames...)...)}
}

func (_c *Mockscanner_ScanNode_Call) Run(run func(scanPolicy *aerospike.ScanPolicy, node *aerospike.Node, namespace string, setName string, binNames ...string)) *Mockscanner_ScanNode_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := args[4].([]string)
		run(args[0].(*aerospike.ScanPolicy), args[1].(*aerospike.Node), args[2].(string), args[3].(string), variadicArgs...)
	})
	return _c
}

func (_c *Mockscanner_ScanNode_Call) Return(recordset *aerospike.Recordset, error aerospike.Error) *Mockscanner_ScanNode_Call {
	_c.Call.Return(recordset, error)
	return _c
}

func (_c *Mockscanner_ScanNode_Call) RunAndReturn(run func(scanPolicy *aerospike.ScanPolicy, node *aerospike.Node, namespace string, setName string, binNames ...string) (*aerospike.Recordset, aerospike.Error)) *Mockscanner_ScanNode_Call {
	_c.Call.Return(run)
	return _c
}

// ScanPartitions provides a mock function for the type Mockscanner
func (_mock *Mockscanner) ScanPartitions(scanPolicy *aerospike.ScanPolicy, partitionFilter *aerospike.PartitionFilter, namespace string, setName string, binNames ...string) (*aerospike.Recordset, aerospike.Error) {
	var tmpRet mock.Arguments
	if len(binNames) > 0 {
		tmpRet = _mock.Called(scanPolicy, partitionFilter, namespace, setName, binNames)
	} else {
		tmpRet = _mock.Called(scanPolicy, partitionFilter, namespace, setName)
	}
	ret := tmpRet

	if len(ret) == 0 {
		panic("no return value specified for ScanPartitions")
	}

	var r0 *aerospike.Recordset
	var r1 aerospike.Error
	if returnFunc, ok := ret.Get(0).(func(*aerospike.ScanPolicy, *aerospike.PartitionFilter, string, string, ...string) (*aerospike.Recordset, aerospike.Error)); ok {
		return returnFunc(scanPolicy, partitionFilter, namespace, setName, binNames...)
	}
	if returnFunc, ok := ret.Get(0).(func(*aerospike.ScanPolicy, *aerospike.PartitionFilter, string, string, ...string) *aerospike.Recordset); ok {
		r0 = returnFunc(scanPolicy, partitionFilter, namespace, setName, binNames...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*aerospike.Recordset)
		}
	}
	if returnFunc, ok := ret.Get(1).(func(*aerospike.ScanPolicy, *aerospike.PartitionFilter, string, string, ...string) aerospike.Error); ok {
		r1 = returnFunc(scanPolicy, partitionFilter, namespace, setName, binNames...)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(aerospike.Error)
		}
	}
	return r0, r1
}

// Mockscanner_ScanPartitions_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ScanPartitions'
type Mockscanner_ScanPartitions_Call struct {
	*mock.Call
}

// ScanPartitions is a helper method to define mock.On call
//   - scanPolicy
//   - partitionFilter
//   - namespace
//   - setName
//   - binNames
func (_e *Mockscanner_Expecter) ScanPartitions(scanPolicy interface{}, partitionFilter interface{}, namespace interface{}, setName interface{}, binNames ...interface{}) *Mockscanner_ScanPartitions_Call {
	return &Mockscanner_ScanPartitions_Call{Call: _e.mock.On("ScanPartitions",
		append([]interface{}{scanPolicy, partitionFilter, namespace, setName}, binNames...)...)}
}

func (_c *Mockscanner_ScanPartitions_Call) Run(run func(scanPolicy *aerospike.ScanPolicy, partitionFilter *aerospike.PartitionFilter, namespace string, setName string, binNames ...string)) *Mockscanner_ScanPartitions_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := args[4].([]string)
		run(args[0].(*aerospike.ScanPolicy), args[1].(*aerospike.PartitionFilter), args[2].(string), args[3].(string), variadicArgs...)
	})
	return _c
}

func (_c *Mockscanner_ScanPartitions_Call) Return(recordset *aerospike.Recordset, error aerospike.Error) *Mockscanner_ScanPartitions_Call {
	_c.Call.Return(recordset, error)
	return _c
}

func (_c *Mockscanner_ScanPartitions_Call) RunAndReturn(run func(scanPolicy *aerospike.ScanPolicy, partitionFilter *aerospike.PartitionFilter, namespace string, setName string, binNames ...string) (*aerospike.Recordset, aerospike.Error)) *Mockscanner_ScanPartitions_Call {
	_c.Call.Return(run)
	return _c
}
