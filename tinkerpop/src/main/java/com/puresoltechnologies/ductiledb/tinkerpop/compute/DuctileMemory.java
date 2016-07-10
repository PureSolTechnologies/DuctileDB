package com.puresoltechnologies.ductiledb.tinkerpop.compute;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MemoryHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class DuctileMemory<R, M> implements Memory.Admin {

    public final Set<MemoryComputeKey> memoryKeys = new HashSet<>();
    public Map<String, Object> previousMap = new ConcurrentHashMap<>();
    public final Map<String, Object> currentMap = new ConcurrentHashMap<>();
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0l);

    public DuctileMemory(VertexProgram<M> vertexProgram, Set<MapReduce<?, ?, ?, ?, R>> mapReducers) {
	if (null != vertexProgram) {
	    for (MemoryComputeKey key : vertexProgram.getMemoryComputeKeys()) {
		// FIXME MemoryHelper.validateKey(key);
		memoryKeys.add(key);
	    }
	}
	for (MapReduce<?, ?, ?, ?, R> mapReduce : mapReducers) {
	    // FIXME memoryKeys.add(mapReduce.getMemoryKey());
	}
    }

    @Override
    public Set<String> keys() {
	return previousMap.keySet();
    }

    @Override
    public void incrIteration() {
	iteration.getAndIncrement();
    }

    @Override
    public void setIteration(int iteration) {
	this.iteration.set(iteration);
    }

    @Override
    public int getIteration() {
	return iteration.get();
    }

    @Override
    public void setRuntime(long runTime) {
	runtime.set(runTime);
    }

    @Override
    public long getRuntime() {
	return runtime.get();
    }

    protected void complete() {
	iteration.decrementAndGet();
	previousMap = currentMap;
    }

    protected void completeSubRound() {
	previousMap = new ConcurrentHashMap<>(currentMap);

    }

    @Override
    public boolean isInitialIteration() {
	return getIteration() == 0;
    }

    @Override
    public <T> T get(String key) throws IllegalArgumentException {
	@SuppressWarnings("unchecked")
	T r = (T) previousMap.get(key);
	if (null == r)
	    throw Memory.Exceptions.memoryDoesNotExist(key);
	else
	    return r;
    }

    @Override
    public void add(String key, Object value) throws IllegalArgumentException, IllegalStateException {
	checkKeyValue(key, value);
	// currentMap.compute(key, (k, v) -> null == v ? value : value + (Long)
	// v);
    }

    @Override
    public void set(String key, Object value) {
	checkKeyValue(key, value);
	currentMap.put(key, value);
    }

    @Override
    public String toString() {
	return StringFactory.memoryString(this);
    }

    private void checkKeyValue(String key, Object value) {
	if (!memoryKeys.contains(key))
	    throw GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey(key);
	MemoryHelper.validateValue(value);
    }

}
