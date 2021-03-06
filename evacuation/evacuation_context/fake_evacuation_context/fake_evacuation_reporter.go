// Code generated by counterfeiter. DO NOT EDIT.
package fake_evacuation_context

import (
	"sync"

	"code.cloudfoundry.org/rep/evacuation/evacuation_context"
)

type FakeEvacuationReporter struct {
	EvacuatingStub        func() bool
	evacuatingMutex       sync.RWMutex
	evacuatingArgsForCall []struct {
	}
	evacuatingReturns struct {
		result1 bool
	}
	evacuatingReturnsOnCall map[int]struct {
		result1 bool
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeEvacuationReporter) Evacuating() bool {
	fake.evacuatingMutex.Lock()
	ret, specificReturn := fake.evacuatingReturnsOnCall[len(fake.evacuatingArgsForCall)]
	fake.evacuatingArgsForCall = append(fake.evacuatingArgsForCall, struct {
	}{})
	fake.recordInvocation("Evacuating", []interface{}{})
	evacuatingStubCopy := fake.EvacuatingStub
	fake.evacuatingMutex.Unlock()
	if evacuatingStubCopy != nil {
		return evacuatingStubCopy()
	}
	if specificReturn {
		return ret.result1
	}
	fakeReturns := fake.evacuatingReturns
	return fakeReturns.result1
}

func (fake *FakeEvacuationReporter) EvacuatingCallCount() int {
	fake.evacuatingMutex.RLock()
	defer fake.evacuatingMutex.RUnlock()
	return len(fake.evacuatingArgsForCall)
}

func (fake *FakeEvacuationReporter) EvacuatingCalls(stub func() bool) {
	fake.evacuatingMutex.Lock()
	defer fake.evacuatingMutex.Unlock()
	fake.EvacuatingStub = stub
}

func (fake *FakeEvacuationReporter) EvacuatingReturns(result1 bool) {
	fake.evacuatingMutex.Lock()
	defer fake.evacuatingMutex.Unlock()
	fake.EvacuatingStub = nil
	fake.evacuatingReturns = struct {
		result1 bool
	}{result1}
}

func (fake *FakeEvacuationReporter) EvacuatingReturnsOnCall(i int, result1 bool) {
	fake.evacuatingMutex.Lock()
	defer fake.evacuatingMutex.Unlock()
	fake.EvacuatingStub = nil
	if fake.evacuatingReturnsOnCall == nil {
		fake.evacuatingReturnsOnCall = make(map[int]struct {
			result1 bool
		})
	}
	fake.evacuatingReturnsOnCall[i] = struct {
		result1 bool
	}{result1}
}

func (fake *FakeEvacuationReporter) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.evacuatingMutex.RLock()
	defer fake.evacuatingMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeEvacuationReporter) recordInvocation(key string, args []interface{}) {
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

var _ evacuation_context.EvacuationReporter = new(FakeEvacuationReporter)
