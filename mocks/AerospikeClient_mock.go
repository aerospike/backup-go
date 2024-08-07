// Code generated by mockery v2.43.2. DO NOT EDIT.

package mocks

import (
	aerospike "github.com/aerospike/aerospike-client-go/v7"

	mock "github.com/stretchr/testify/mock"
)

// MockAerospikeClient is an autogenerated mock type for the AerospikeClient type
type MockAerospikeClient struct {
	mock.Mock
}

type MockAerospikeClient_Expecter struct {
	mock *mock.Mock
}

func (_m *MockAerospikeClient) EXPECT() *MockAerospikeClient_Expecter {
	return &MockAerospikeClient_Expecter{mock: &_m.Mock}
}

// BatchOperate provides a mock function with given fields: policy, records
func (_m *MockAerospikeClient) BatchOperate(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
	ret := _m.Called(policy, records)

	if len(ret) == 0 {
		panic("no return value specified for BatchOperate")
	}

	var r0 aerospike.Error
	if rf, ok := ret.Get(0).(func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error); ok {
		r0 = rf(policy, records)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(aerospike.Error)
		}
	}

	return r0
}

// MockAerospikeClient_BatchOperate_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'BatchOperate'
type MockAerospikeClient_BatchOperate_Call struct {
	*mock.Call
}

// BatchOperate is a helper method to define mock.On call
//   - policy *aerospike.BatchPolicy
//   - records []aerospike.BatchRecordIfc
func (_e *MockAerospikeClient_Expecter) BatchOperate(policy interface{}, records interface{}) *MockAerospikeClient_BatchOperate_Call {
	return &MockAerospikeClient_BatchOperate_Call{Call: _e.mock.On("BatchOperate", policy, records)}
}

func (_c *MockAerospikeClient_BatchOperate_Call) Run(run func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc)) *MockAerospikeClient_BatchOperate_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*aerospike.BatchPolicy), args[1].([]aerospike.BatchRecordIfc))
	})
	return _c
}

func (_c *MockAerospikeClient_BatchOperate_Call) Return(_a0 aerospike.Error) *MockAerospikeClient_BatchOperate_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockAerospikeClient_BatchOperate_Call) RunAndReturn(run func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error) *MockAerospikeClient_BatchOperate_Call {
	_c.Call.Return(run)
	return _c
}

// Cluster provides a mock function with given fields:
func (_m *MockAerospikeClient) Cluster() *aerospike.Cluster {
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

// MockAerospikeClient_Cluster_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Cluster'
type MockAerospikeClient_Cluster_Call struct {
	*mock.Call
}

// Cluster is a helper method to define mock.On call
func (_e *MockAerospikeClient_Expecter) Cluster() *MockAerospikeClient_Cluster_Call {
	return &MockAerospikeClient_Cluster_Call{Call: _e.mock.On("Cluster")}
}

func (_c *MockAerospikeClient_Cluster_Call) Run(run func()) *MockAerospikeClient_Cluster_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockAerospikeClient_Cluster_Call) Return(_a0 *aerospike.Cluster) *MockAerospikeClient_Cluster_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockAerospikeClient_Cluster_Call) RunAndReturn(run func() *aerospike.Cluster) *MockAerospikeClient_Cluster_Call {
	_c.Call.Return(run)
	return _c
}

// CreateComplexIndex provides a mock function with given fields: policy, namespace, set, indexName, binName, indexType, indexCollectionType, ctx
func (_m *MockAerospikeClient) CreateComplexIndex(policy *aerospike.WritePolicy, namespace string, set string, indexName string, binName string, indexType aerospike.IndexType, indexCollectionType aerospike.IndexCollectionType, ctx ...*aerospike.CDTContext) (*aerospike.IndexTask, aerospike.Error) {
	_va := make([]interface{}, len(ctx))
	for _i := range ctx {
		_va[_i] = ctx[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, policy, namespace, set, indexName, binName, indexType, indexCollectionType)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for CreateComplexIndex")
	}

	var r0 *aerospike.IndexTask
	var r1 aerospike.Error
	if rf, ok := ret.Get(0).(func(*aerospike.WritePolicy, string, string, string, string, aerospike.IndexType, aerospike.IndexCollectionType, ...*aerospike.CDTContext) (*aerospike.IndexTask, aerospike.Error)); ok {
		return rf(policy, namespace, set, indexName, binName, indexType, indexCollectionType, ctx...)
	}
	if rf, ok := ret.Get(0).(func(*aerospike.WritePolicy, string, string, string, string, aerospike.IndexType, aerospike.IndexCollectionType, ...*aerospike.CDTContext) *aerospike.IndexTask); ok {
		r0 = rf(policy, namespace, set, indexName, binName, indexType, indexCollectionType, ctx...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*aerospike.IndexTask)
		}
	}

	if rf, ok := ret.Get(1).(func(*aerospike.WritePolicy, string, string, string, string, aerospike.IndexType, aerospike.IndexCollectionType, ...*aerospike.CDTContext) aerospike.Error); ok {
		r1 = rf(policy, namespace, set, indexName, binName, indexType, indexCollectionType, ctx...)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(aerospike.Error)
		}
	}

	return r0, r1
}

// MockAerospikeClient_CreateComplexIndex_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CreateComplexIndex'
type MockAerospikeClient_CreateComplexIndex_Call struct {
	*mock.Call
}

// CreateComplexIndex is a helper method to define mock.On call
//   - policy *aerospike.WritePolicy
//   - namespace string
//   - set string
//   - indexName string
//   - binName string
//   - indexType aerospike.IndexType
//   - indexCollectionType aerospike.IndexCollectionType
//   - ctx ...*aerospike.CDTContext
func (_e *MockAerospikeClient_Expecter) CreateComplexIndex(policy interface{}, namespace interface{}, set interface{}, indexName interface{}, binName interface{}, indexType interface{}, indexCollectionType interface{}, ctx ...interface{}) *MockAerospikeClient_CreateComplexIndex_Call {
	return &MockAerospikeClient_CreateComplexIndex_Call{Call: _e.mock.On("CreateComplexIndex",
		append([]interface{}{policy, namespace, set, indexName, binName, indexType, indexCollectionType}, ctx...)...)}
}

func (_c *MockAerospikeClient_CreateComplexIndex_Call) Run(run func(policy *aerospike.WritePolicy, namespace string, set string, indexName string, binName string, indexType aerospike.IndexType, indexCollectionType aerospike.IndexCollectionType, ctx ...*aerospike.CDTContext)) *MockAerospikeClient_CreateComplexIndex_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]*aerospike.CDTContext, len(args)-7)
		for i, a := range args[7:] {
			if a != nil {
				variadicArgs[i] = a.(*aerospike.CDTContext)
			}
		}
		run(args[0].(*aerospike.WritePolicy), args[1].(string), args[2].(string), args[3].(string), args[4].(string), args[5].(aerospike.IndexType), args[6].(aerospike.IndexCollectionType), variadicArgs...)
	})
	return _c
}

func (_c *MockAerospikeClient_CreateComplexIndex_Call) Return(_a0 *aerospike.IndexTask, _a1 aerospike.Error) *MockAerospikeClient_CreateComplexIndex_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockAerospikeClient_CreateComplexIndex_Call) RunAndReturn(run func(*aerospike.WritePolicy, string, string, string, string, aerospike.IndexType, aerospike.IndexCollectionType, ...*aerospike.CDTContext) (*aerospike.IndexTask, aerospike.Error)) *MockAerospikeClient_CreateComplexIndex_Call {
	_c.Call.Return(run)
	return _c
}

// DropIndex provides a mock function with given fields: policy, namespace, set, indexName
func (_m *MockAerospikeClient) DropIndex(policy *aerospike.WritePolicy, namespace string, set string, indexName string) aerospike.Error {
	ret := _m.Called(policy, namespace, set, indexName)

	if len(ret) == 0 {
		panic("no return value specified for DropIndex")
	}

	var r0 aerospike.Error
	if rf, ok := ret.Get(0).(func(*aerospike.WritePolicy, string, string, string) aerospike.Error); ok {
		r0 = rf(policy, namespace, set, indexName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(aerospike.Error)
		}
	}

	return r0
}

// MockAerospikeClient_DropIndex_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DropIndex'
type MockAerospikeClient_DropIndex_Call struct {
	*mock.Call
}

// DropIndex is a helper method to define mock.On call
//   - policy *aerospike.WritePolicy
//   - namespace string
//   - set string
//   - indexName string
func (_e *MockAerospikeClient_Expecter) DropIndex(policy interface{}, namespace interface{}, set interface{}, indexName interface{}) *MockAerospikeClient_DropIndex_Call {
	return &MockAerospikeClient_DropIndex_Call{Call: _e.mock.On("DropIndex", policy, namespace, set, indexName)}
}

func (_c *MockAerospikeClient_DropIndex_Call) Run(run func(policy *aerospike.WritePolicy, namespace string, set string, indexName string)) *MockAerospikeClient_DropIndex_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*aerospike.WritePolicy), args[1].(string), args[2].(string), args[3].(string))
	})
	return _c
}

func (_c *MockAerospikeClient_DropIndex_Call) Return(_a0 aerospike.Error) *MockAerospikeClient_DropIndex_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockAerospikeClient_DropIndex_Call) RunAndReturn(run func(*aerospike.WritePolicy, string, string, string) aerospike.Error) *MockAerospikeClient_DropIndex_Call {
	_c.Call.Return(run)
	return _c
}

// GetDefaultInfoPolicy provides a mock function with given fields:
func (_m *MockAerospikeClient) GetDefaultInfoPolicy() *aerospike.InfoPolicy {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetDefaultInfoPolicy")
	}

	var r0 *aerospike.InfoPolicy
	if rf, ok := ret.Get(0).(func() *aerospike.InfoPolicy); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*aerospike.InfoPolicy)
		}
	}

	return r0
}

// MockAerospikeClient_GetDefaultInfoPolicy_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetDefaultInfoPolicy'
type MockAerospikeClient_GetDefaultInfoPolicy_Call struct {
	*mock.Call
}

// GetDefaultInfoPolicy is a helper method to define mock.On call
func (_e *MockAerospikeClient_Expecter) GetDefaultInfoPolicy() *MockAerospikeClient_GetDefaultInfoPolicy_Call {
	return &MockAerospikeClient_GetDefaultInfoPolicy_Call{Call: _e.mock.On("GetDefaultInfoPolicy")}
}

func (_c *MockAerospikeClient_GetDefaultInfoPolicy_Call) Run(run func()) *MockAerospikeClient_GetDefaultInfoPolicy_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockAerospikeClient_GetDefaultInfoPolicy_Call) Return(_a0 *aerospike.InfoPolicy) *MockAerospikeClient_GetDefaultInfoPolicy_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockAerospikeClient_GetDefaultInfoPolicy_Call) RunAndReturn(run func() *aerospike.InfoPolicy) *MockAerospikeClient_GetDefaultInfoPolicy_Call {
	_c.Call.Return(run)
	return _c
}

// GetDefaultScanPolicy provides a mock function with given fields:
func (_m *MockAerospikeClient) GetDefaultScanPolicy() *aerospike.ScanPolicy {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetDefaultScanPolicy")
	}

	var r0 *aerospike.ScanPolicy
	if rf, ok := ret.Get(0).(func() *aerospike.ScanPolicy); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*aerospike.ScanPolicy)
		}
	}

	return r0
}

// MockAerospikeClient_GetDefaultScanPolicy_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetDefaultScanPolicy'
type MockAerospikeClient_GetDefaultScanPolicy_Call struct {
	*mock.Call
}

// GetDefaultScanPolicy is a helper method to define mock.On call
func (_e *MockAerospikeClient_Expecter) GetDefaultScanPolicy() *MockAerospikeClient_GetDefaultScanPolicy_Call {
	return &MockAerospikeClient_GetDefaultScanPolicy_Call{Call: _e.mock.On("GetDefaultScanPolicy")}
}

func (_c *MockAerospikeClient_GetDefaultScanPolicy_Call) Run(run func()) *MockAerospikeClient_GetDefaultScanPolicy_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockAerospikeClient_GetDefaultScanPolicy_Call) Return(_a0 *aerospike.ScanPolicy) *MockAerospikeClient_GetDefaultScanPolicy_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockAerospikeClient_GetDefaultScanPolicy_Call) RunAndReturn(run func() *aerospike.ScanPolicy) *MockAerospikeClient_GetDefaultScanPolicy_Call {
	_c.Call.Return(run)
	return _c
}

// GetDefaultWritePolicy provides a mock function with given fields:
func (_m *MockAerospikeClient) GetDefaultWritePolicy() *aerospike.WritePolicy {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetDefaultWritePolicy")
	}

	var r0 *aerospike.WritePolicy
	if rf, ok := ret.Get(0).(func() *aerospike.WritePolicy); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*aerospike.WritePolicy)
		}
	}

	return r0
}

// MockAerospikeClient_GetDefaultWritePolicy_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetDefaultWritePolicy'
type MockAerospikeClient_GetDefaultWritePolicy_Call struct {
	*mock.Call
}

// GetDefaultWritePolicy is a helper method to define mock.On call
func (_e *MockAerospikeClient_Expecter) GetDefaultWritePolicy() *MockAerospikeClient_GetDefaultWritePolicy_Call {
	return &MockAerospikeClient_GetDefaultWritePolicy_Call{Call: _e.mock.On("GetDefaultWritePolicy")}
}

func (_c *MockAerospikeClient_GetDefaultWritePolicy_Call) Run(run func()) *MockAerospikeClient_GetDefaultWritePolicy_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockAerospikeClient_GetDefaultWritePolicy_Call) Return(_a0 *aerospike.WritePolicy) *MockAerospikeClient_GetDefaultWritePolicy_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockAerospikeClient_GetDefaultWritePolicy_Call) RunAndReturn(run func() *aerospike.WritePolicy) *MockAerospikeClient_GetDefaultWritePolicy_Call {
	_c.Call.Return(run)
	return _c
}

// Put provides a mock function with given fields: policy, key, bins
func (_m *MockAerospikeClient) Put(policy *aerospike.WritePolicy, key *aerospike.Key, bins aerospike.BinMap) aerospike.Error {
	ret := _m.Called(policy, key, bins)

	if len(ret) == 0 {
		panic("no return value specified for Put")
	}

	var r0 aerospike.Error
	if rf, ok := ret.Get(0).(func(*aerospike.WritePolicy, *aerospike.Key, aerospike.BinMap) aerospike.Error); ok {
		r0 = rf(policy, key, bins)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(aerospike.Error)
		}
	}

	return r0
}

// MockAerospikeClient_Put_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Put'
type MockAerospikeClient_Put_Call struct {
	*mock.Call
}

// Put is a helper method to define mock.On call
//   - policy *aerospike.WritePolicy
//   - key *aerospike.Key
//   - bins aerospike.BinMap
func (_e *MockAerospikeClient_Expecter) Put(policy interface{}, key interface{}, bins interface{}) *MockAerospikeClient_Put_Call {
	return &MockAerospikeClient_Put_Call{Call: _e.mock.On("Put", policy, key, bins)}
}

func (_c *MockAerospikeClient_Put_Call) Run(run func(policy *aerospike.WritePolicy, key *aerospike.Key, bins aerospike.BinMap)) *MockAerospikeClient_Put_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*aerospike.WritePolicy), args[1].(*aerospike.Key), args[2].(aerospike.BinMap))
	})
	return _c
}

func (_c *MockAerospikeClient_Put_Call) Return(_a0 aerospike.Error) *MockAerospikeClient_Put_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockAerospikeClient_Put_Call) RunAndReturn(run func(*aerospike.WritePolicy, *aerospike.Key, aerospike.BinMap) aerospike.Error) *MockAerospikeClient_Put_Call {
	_c.Call.Return(run)
	return _c
}

// RegisterUDF provides a mock function with given fields: policy, udfBody, serverPath, language
func (_m *MockAerospikeClient) RegisterUDF(policy *aerospike.WritePolicy, udfBody []byte, serverPath string, language aerospike.Language) (*aerospike.RegisterTask, aerospike.Error) {
	ret := _m.Called(policy, udfBody, serverPath, language)

	if len(ret) == 0 {
		panic("no return value specified for RegisterUDF")
	}

	var r0 *aerospike.RegisterTask
	var r1 aerospike.Error
	if rf, ok := ret.Get(0).(func(*aerospike.WritePolicy, []byte, string, aerospike.Language) (*aerospike.RegisterTask, aerospike.Error)); ok {
		return rf(policy, udfBody, serverPath, language)
	}
	if rf, ok := ret.Get(0).(func(*aerospike.WritePolicy, []byte, string, aerospike.Language) *aerospike.RegisterTask); ok {
		r0 = rf(policy, udfBody, serverPath, language)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*aerospike.RegisterTask)
		}
	}

	if rf, ok := ret.Get(1).(func(*aerospike.WritePolicy, []byte, string, aerospike.Language) aerospike.Error); ok {
		r1 = rf(policy, udfBody, serverPath, language)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(aerospike.Error)
		}
	}

	return r0, r1
}

// MockAerospikeClient_RegisterUDF_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RegisterUDF'
type MockAerospikeClient_RegisterUDF_Call struct {
	*mock.Call
}

// RegisterUDF is a helper method to define mock.On call
//   - policy *aerospike.WritePolicy
//   - udfBody []byte
//   - serverPath string
//   - language aerospike.Language
func (_e *MockAerospikeClient_Expecter) RegisterUDF(policy interface{}, udfBody interface{}, serverPath interface{}, language interface{}) *MockAerospikeClient_RegisterUDF_Call {
	return &MockAerospikeClient_RegisterUDF_Call{Call: _e.mock.On("RegisterUDF", policy, udfBody, serverPath, language)}
}

func (_c *MockAerospikeClient_RegisterUDF_Call) Run(run func(policy *aerospike.WritePolicy, udfBody []byte, serverPath string, language aerospike.Language)) *MockAerospikeClient_RegisterUDF_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*aerospike.WritePolicy), args[1].([]byte), args[2].(string), args[3].(aerospike.Language))
	})
	return _c
}

func (_c *MockAerospikeClient_RegisterUDF_Call) Return(_a0 *aerospike.RegisterTask, _a1 aerospike.Error) *MockAerospikeClient_RegisterUDF_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockAerospikeClient_RegisterUDF_Call) RunAndReturn(run func(*aerospike.WritePolicy, []byte, string, aerospike.Language) (*aerospike.RegisterTask, aerospike.Error)) *MockAerospikeClient_RegisterUDF_Call {
	_c.Call.Return(run)
	return _c
}

// ScanPartitions provides a mock function with given fields: scanPolicy, partitionFilter, namespace, setName, binNames
func (_m *MockAerospikeClient) ScanPartitions(scanPolicy *aerospike.ScanPolicy, partitionFilter *aerospike.PartitionFilter, namespace string, setName string, binNames ...string) (*aerospike.Recordset, aerospike.Error) {
	_va := make([]interface{}, len(binNames))
	for _i := range binNames {
		_va[_i] = binNames[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, scanPolicy, partitionFilter, namespace, setName)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for ScanPartitions")
	}

	var r0 *aerospike.Recordset
	var r1 aerospike.Error
	if rf, ok := ret.Get(0).(func(*aerospike.ScanPolicy, *aerospike.PartitionFilter, string, string, ...string) (*aerospike.Recordset, aerospike.Error)); ok {
		return rf(scanPolicy, partitionFilter, namespace, setName, binNames...)
	}
	if rf, ok := ret.Get(0).(func(*aerospike.ScanPolicy, *aerospike.PartitionFilter, string, string, ...string) *aerospike.Recordset); ok {
		r0 = rf(scanPolicy, partitionFilter, namespace, setName, binNames...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*aerospike.Recordset)
		}
	}

	if rf, ok := ret.Get(1).(func(*aerospike.ScanPolicy, *aerospike.PartitionFilter, string, string, ...string) aerospike.Error); ok {
		r1 = rf(scanPolicy, partitionFilter, namespace, setName, binNames...)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(aerospike.Error)
		}
	}

	return r0, r1
}

// MockAerospikeClient_ScanPartitions_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ScanPartitions'
type MockAerospikeClient_ScanPartitions_Call struct {
	*mock.Call
}

// ScanPartitions is a helper method to define mock.On call
//   - scanPolicy *aerospike.ScanPolicy
//   - partitionFilter *aerospike.PartitionFilter
//   - namespace string
//   - setName string
//   - binNames ...string
func (_e *MockAerospikeClient_Expecter) ScanPartitions(scanPolicy interface{}, partitionFilter interface{}, namespace interface{}, setName interface{}, binNames ...interface{}) *MockAerospikeClient_ScanPartitions_Call {
	return &MockAerospikeClient_ScanPartitions_Call{Call: _e.mock.On("ScanPartitions",
		append([]interface{}{scanPolicy, partitionFilter, namespace, setName}, binNames...)...)}
}

func (_c *MockAerospikeClient_ScanPartitions_Call) Run(run func(scanPolicy *aerospike.ScanPolicy, partitionFilter *aerospike.PartitionFilter, namespace string, setName string, binNames ...string)) *MockAerospikeClient_ScanPartitions_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]string, len(args)-4)
		for i, a := range args[4:] {
			if a != nil {
				variadicArgs[i] = a.(string)
			}
		}
		run(args[0].(*aerospike.ScanPolicy), args[1].(*aerospike.PartitionFilter), args[2].(string), args[3].(string), variadicArgs...)
	})
	return _c
}

func (_c *MockAerospikeClient_ScanPartitions_Call) Return(_a0 *aerospike.Recordset, _a1 aerospike.Error) *MockAerospikeClient_ScanPartitions_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockAerospikeClient_ScanPartitions_Call) RunAndReturn(run func(*aerospike.ScanPolicy, *aerospike.PartitionFilter, string, string, ...string) (*aerospike.Recordset, aerospike.Error)) *MockAerospikeClient_ScanPartitions_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockAerospikeClient creates a new instance of MockAerospikeClient. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockAerospikeClient(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockAerospikeClient {
	mock := &MockAerospikeClient{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}