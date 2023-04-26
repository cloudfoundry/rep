// Code generated by counterfeiter. DO NOT EDIT.
package fake_generator

import (
	"sync"

	lager "code.cloudfoundry.org/lager/v3"
	"code.cloudfoundry.org/operationq"
	"code.cloudfoundry.org/rep/generator"
)

type FakeGenerator struct {
	BatchOperationsStub        func(lager.Logger) (map[string]operationq.Operation, error)
	batchOperationsMutex       sync.RWMutex
	batchOperationsArgsForCall []struct {
		arg1 lager.Logger
	}
	batchOperationsReturns struct {
		result1 map[string]operationq.Operation
		result2 error
	}
	batchOperationsReturnsOnCall map[int]struct {
		result1 map[string]operationq.Operation
		result2 error
	}
	OperationStreamStub        func(lager.Logger) (<-chan operationq.Operation, error)
	operationStreamMutex       sync.RWMutex
	operationStreamArgsForCall []struct {
		arg1 lager.Logger
	}
	operationStreamReturns struct {
		result1 <-chan operationq.Operation
		result2 error
	}
	operationStreamReturnsOnCall map[int]struct {
		result1 <-chan operationq.Operation
		result2 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeGenerator) BatchOperations(arg1 lager.Logger) (map[string]operationq.Operation, error) {
	fake.batchOperationsMutex.Lock()
	ret, specificReturn := fake.batchOperationsReturnsOnCall[len(fake.batchOperationsArgsForCall)]
	fake.batchOperationsArgsForCall = append(fake.batchOperationsArgsForCall, struct {
		arg1 lager.Logger
	}{arg1})
	stub := fake.BatchOperationsStub
	fakeReturns := fake.batchOperationsReturns
	fake.recordInvocation("BatchOperations", []interface{}{arg1})
	fake.batchOperationsMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeGenerator) BatchOperationsCallCount() int {
	fake.batchOperationsMutex.RLock()
	defer fake.batchOperationsMutex.RUnlock()
	return len(fake.batchOperationsArgsForCall)
}

func (fake *FakeGenerator) BatchOperationsCalls(stub func(lager.Logger) (map[string]operationq.Operation, error)) {
	fake.batchOperationsMutex.Lock()
	defer fake.batchOperationsMutex.Unlock()
	fake.BatchOperationsStub = stub
}

func (fake *FakeGenerator) BatchOperationsArgsForCall(i int) lager.Logger {
	fake.batchOperationsMutex.RLock()
	defer fake.batchOperationsMutex.RUnlock()
	argsForCall := fake.batchOperationsArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeGenerator) BatchOperationsReturns(result1 map[string]operationq.Operation, result2 error) {
	fake.batchOperationsMutex.Lock()
	defer fake.batchOperationsMutex.Unlock()
	fake.BatchOperationsStub = nil
	fake.batchOperationsReturns = struct {
		result1 map[string]operationq.Operation
		result2 error
	}{result1, result2}
}

func (fake *FakeGenerator) BatchOperationsReturnsOnCall(i int, result1 map[string]operationq.Operation, result2 error) {
	fake.batchOperationsMutex.Lock()
	defer fake.batchOperationsMutex.Unlock()
	fake.BatchOperationsStub = nil
	if fake.batchOperationsReturnsOnCall == nil {
		fake.batchOperationsReturnsOnCall = make(map[int]struct {
			result1 map[string]operationq.Operation
			result2 error
		})
	}
	fake.batchOperationsReturnsOnCall[i] = struct {
		result1 map[string]operationq.Operation
		result2 error
	}{result1, result2}
}

func (fake *FakeGenerator) OperationStream(arg1 lager.Logger) (<-chan operationq.Operation, error) {
	fake.operationStreamMutex.Lock()
	ret, specificReturn := fake.operationStreamReturnsOnCall[len(fake.operationStreamArgsForCall)]
	fake.operationStreamArgsForCall = append(fake.operationStreamArgsForCall, struct {
		arg1 lager.Logger
	}{arg1})
	stub := fake.OperationStreamStub
	fakeReturns := fake.operationStreamReturns
	fake.recordInvocation("OperationStream", []interface{}{arg1})
	fake.operationStreamMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeGenerator) OperationStreamCallCount() int {
	fake.operationStreamMutex.RLock()
	defer fake.operationStreamMutex.RUnlock()
	return len(fake.operationStreamArgsForCall)
}

func (fake *FakeGenerator) OperationStreamCalls(stub func(lager.Logger) (<-chan operationq.Operation, error)) {
	fake.operationStreamMutex.Lock()
	defer fake.operationStreamMutex.Unlock()
	fake.OperationStreamStub = stub
}

func (fake *FakeGenerator) OperationStreamArgsForCall(i int) lager.Logger {
	fake.operationStreamMutex.RLock()
	defer fake.operationStreamMutex.RUnlock()
	argsForCall := fake.operationStreamArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeGenerator) OperationStreamReturns(result1 <-chan operationq.Operation, result2 error) {
	fake.operationStreamMutex.Lock()
	defer fake.operationStreamMutex.Unlock()
	fake.OperationStreamStub = nil
	fake.operationStreamReturns = struct {
		result1 <-chan operationq.Operation
		result2 error
	}{result1, result2}
}

func (fake *FakeGenerator) OperationStreamReturnsOnCall(i int, result1 <-chan operationq.Operation, result2 error) {
	fake.operationStreamMutex.Lock()
	defer fake.operationStreamMutex.Unlock()
	fake.OperationStreamStub = nil
	if fake.operationStreamReturnsOnCall == nil {
		fake.operationStreamReturnsOnCall = make(map[int]struct {
			result1 <-chan operationq.Operation
			result2 error
		})
	}
	fake.operationStreamReturnsOnCall[i] = struct {
		result1 <-chan operationq.Operation
		result2 error
	}{result1, result2}
}

func (fake *FakeGenerator) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.batchOperationsMutex.RLock()
	defer fake.batchOperationsMutex.RUnlock()
	fake.operationStreamMutex.RLock()
	defer fake.operationStreamMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeGenerator) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}

var _ generator.Generator = new(FakeGenerator)
