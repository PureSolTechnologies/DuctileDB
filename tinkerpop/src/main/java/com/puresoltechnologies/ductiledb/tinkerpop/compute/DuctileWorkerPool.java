package com.puresoltechnologies.ductiledb.tinkerpop.compute;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MapReducePool;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramPool;

import io.netty.util.concurrent.DefaultThreadFactory;

public class DuctileWorkerPool<R, M> implements AutoCloseable {

    private static final DefaultThreadFactory THREAD_FACTORY_WORKER = new DefaultThreadFactory("ductile-worker-%d");

    private final int numberOfWorkers;
    private final ExecutorService workerPool;
    private final CompletionService<R> completionService;

    private VertexProgramPool vertexProgramPool = null;
    private MapReducePool mapReducePool = null;

    public DuctileWorkerPool(int numberOfWorkers) {
	this.numberOfWorkers = numberOfWorkers;
	workerPool = Executors.newFixedThreadPool(numberOfWorkers, THREAD_FACTORY_WORKER);
	completionService = new ExecutorCompletionService<>(workerPool);
    }

    public void setVertexProgram(VertexProgram<M> vertexProgram) {
	vertexProgramPool = new VertexProgramPool(vertexProgram, numberOfWorkers);
    }

    public void setMapReduce(MapReduce<?, ?, ?, ?, R> mapReduce) {
	mapReducePool = new MapReducePool(mapReduce, numberOfWorkers);
    }

    public void executeVertexProgram(Consumer<VertexProgram<M>> worker) {
	for (int i = 0; i < numberOfWorkers; i++) {
	    completionService.submit(() -> {
		@SuppressWarnings("unchecked")
		VertexProgram<M> vp = vertexProgramPool.take();
		worker.accept(vp);
		vertexProgramPool.offer(vp);
		return null;
	    });
	}
	for (int i = 0; i < numberOfWorkers; i++) {
	    try {
		completionService.take().get();
	    } catch (Exception e) {
		throw new IllegalStateException(e.getMessage(), e);
	    }
	}
    }

    public void executeMapReduce(Consumer<MapReduce<?, ?, ?, ?, R>> worker) {
	for (int i = 0; i < numberOfWorkers; i++) {
	    completionService.submit(() -> {
		@SuppressWarnings("unchecked")
		MapReduce<?, ?, ?, ?, R> mr = mapReducePool.take();
		worker.accept(mr);
		mapReducePool.offer(mr);
		return null;
	    });
	}
	for (int i = 0; i < numberOfWorkers; i++) {
	    try {
		completionService.take().get();
	    } catch (Exception e) {
		throw new IllegalStateException(e.getMessage(), e);
	    }
	}
    }

    @Override
    public void close() throws Exception {
	workerPool.shutdown();
    }

}
