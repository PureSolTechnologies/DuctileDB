package com.puresoltechnologies.ductiledb.tinkerpop.compute;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;

public class DuctileGraphComputer implements GraphComputer {

    private ResultGraph resultGraph = null;
    private Persist persist = null;

    private VertexProgram<?> vertexProgram;
    private final DuctileGraph graph;
    private DuctileMemory<?, ?> memory;
    private final DuctileMessageBoard<?> messageBoard = new DuctileMessageBoard<>();
    private boolean executed = false;
    private final Set<MapReduce<?, ?, ?, ?, ?>> mapReducers = new HashSet<>();
    private int workers = Runtime.getRuntime().availableProcessors();

    public DuctileGraphComputer(DuctileGraph graph) {
	this.graph = graph;
    }

    @Override
    public GraphComputer result(final ResultGraph resultGraph) {
	this.resultGraph = resultGraph;
	return this;
    }

    @Override
    public GraphComputer persist(final Persist persist) {
	this.persist = persist;
	return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public GraphComputer program(@SuppressWarnings("rawtypes") final VertexProgram vertexProgram) {
	this.vertexProgram = vertexProgram;
	return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public GraphComputer mapReduce(@SuppressWarnings("rawtypes") final MapReduce mapReduce) {
	this.mapReducers.add(mapReduce);
	return this;
    }

    @Override
    public GraphComputer workers(final int workers) {
	this.workers = workers;
	return this;
    }

    @Override
    public Future<ComputerResult> submit() {
	// FIXME
	return null;
	// // a graph computer can only be executed once
	// if (this.executed)
	// throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
	// else
	// this.executed = true;
	// // it is not possible execute a computer if it has no vertex program
	// nor
	// // mapreducers
	// if (null == this.vertexProgram && this.mapReducers.isEmpty())
	// throw
	// GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers();
	// // it is possible to run mapreducers without a vertex program
	// if (null != this.vertexProgram) {
	// GraphComputerHelper.validateProgramOnComputer(this,
	// this.vertexProgram);
	// @SuppressWarnings("unchecked")
	// Collection<? extends MapReduce<?, ?, ?, ?, ?>> mrs = (Collection<?
	// extends MapReduce<?, ?, ?, ?, ?>>) this.vertexProgram
	// .getMapReducers();
	// this.mapReducers.addAll(mrs);
	// }
	// // get the result graph and persist state to use for the computation
	// this.resultGraph =
	// GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram),
	// Optional.ofNullable(this.resultGraph));
	// this.persist =
	// GraphComputerHelper.getPersistState(Optional.ofNullable(this.vertexProgram),
	// Optional.ofNullable(this.persist));
	// if
	// (!this.features().supportsResultGraphPersistCombination(this.resultGraph,
	// this.persist))
	// throw
	// GraphComputer.Exceptions.resultGraphPersistCombinationNotSupported(this.resultGraph,
	// this.persist);
	// // ensure requested workers are not larger than supported workers
	// if (this.workers > this.features().getMaxWorkers())
	// throw
	// GraphComputer.Exceptions.computerRequiresMoreWorkersThanSupported(this.workers,
	// this.features().getMaxWorkers());
	//
	// // initialize the memory
	// this.memory = new DuctileMemory(this.vertexProgram,
	// this.mapReducers);
	// return CompletableFuture.<ComputerResult> supplyAsync(() -> {
	// final long time = System.currentTimeMillis();
	// try (final DuctileWorkerPool<?, ?> workers = new
	// DuctileWorkerPool<>(this.workers)) {
	// if (null != this.vertexProgram) {
	// graph.createGraphComputerView(vertexProgram.getElementComputeKeys());
	// // execute the vertex program
	// this.vertexProgram.setup(this.memory);
	// this.memory.completeSubRound();
	// while (true) {
	// workers.setVertexProgram(vertexProgram);
	// final SynchronizedIterator<Vertex> vertices = new
	// SynchronizedIterator<>(this.graph.vertices());
	// workers.executeVertexProgram(vertexProgram -> {
	// vertexProgram.workerIterationStart(this.memory.asImmutable());
	// while (true) {
	// final Vertex vertex = vertices.next();
	// if (null == vertex)
	// break;
	// DuctileMessenger<M> messenger = new DuctileMessenger<>(vertex,
	// messageBoard,
	// vertexProgram.getMessageCombiner());
	// vertexProgram.execute(ComputerGraph.vertexProgram(vertex,
	// vertexProgram), messenger,
	// memory);
	// }
	// vertexProgram.workerIterationEnd(this.memory.asImmutable());
	// });
	// this.messageBoard.completeIteration();
	// this.memory.completeSubRound();
	// if (this.vertexProgram.terminate(this.memory)) {
	// this.memory.incrIteration();
	// this.memory.completeSubRound();
	// break;
	// } else {
	// this.memory.incrIteration();
	// this.memory.completeSubRound();
	// }
	// }
	// }
	//
	// // execute mapreduce jobs
	// for (final MapReduce<?, ?, ?, ?, ?> mapReduce : mapReducers) {
	// if (mapReduce.doStage(MapReduce.Stage.MAP)) {
	// final DuctileMapEmitter<?, ?> mapEmitter = new DuctileMapEmitter<>(
	// mapReduce.doStage(MapReduce.Stage.REDUCE));
	// final SynchronizedIterator<Vertex> vertices = new
	// SynchronizedIterator<>(this.graph.vertices());
	// workers.setMapReduce(mapReduce);
	// workers.executeMapReduce(workerMapReduce -> {
	// workerMapReduce.workerStart(MapReduce.Stage.MAP);
	// while (true) {
	// final Vertex vertex = vertices.next();
	// if (null == vertex)
	// break;
	// workerMapReduce.map(ComputerGraph.mapReduce(vertex), mapEmitter);
	// }
	// workerMapReduce.workerEnd(MapReduce.Stage.MAP);
	// });
	// // sort results if a map output sort is defined
	// mapEmitter.complete((MapReduce<?, ?, ?, ?, ?>) mapReduce);
	//
	// // no need to run combiners as this is single machine
	// if (mapReduce.doStage(MapReduce.Stage.REDUCE)) {
	// final DuctileReduceEmitter<?, ?> reduceEmitter = new
	// DuctileReduceEmitter<>();
	// @SuppressWarnings({ "unchecked", "rawtypes" })
	// final SynchronizedIterator<Map.Entry<?, Queue<?>>> keyValues = new
	// SynchronizedIterator(
	// mapEmitter.reduceMap.entrySet().iterator());
	// workers.executeMapReduce(workerMapReduce -> {
	// workerMapReduce.workerStart(MapReduce.Stage.REDUCE);
	// while (true) {
	// final Map.Entry<?, Queue<?>> entry = keyValues.next();
	// if (null == entry)
	// break;
	// workerMapReduce.reduce(entry.getKey(), entry.getValue().iterator(),
	// reduceEmitter);
	// }
	// workerMapReduce.workerEnd(MapReduce.Stage.REDUCE);
	// });
	// reduceEmitter.complete(mapReduce); // sort results
	// // if a reduce
	// // output sort is
	// // defined
	// mapReduce.addResultToMemory(memory,
	// reduceEmitter.reduceQueue.iterator());
	// } else {
	// mapReduce.addResultToMemory(memory, mapEmitter.mapQueue.iterator());
	// }
	// }
	// }
	// // update runtime and return the newly computed graph
	// this.memory.setRuntime(System.currentTimeMillis() - time);
	// this.memory.complete();
	// // determine the resultant graph based on the result
	// // graph/persist state
	// DuctileGraphComputerView view = graph.getGraphComputerView();
	// final Graph resultGraph = null == view ? this.graph
	// : view.processResultGraphPersist(this.resultGraph, this.persist);
	// graph.dropGraphComputerView();
	// return new DefaultComputerResult(resultGraph,
	// this.memory.asImmutable());
	//
	// } catch (Exception ex) {
	// throw new RuntimeException(ex);
	// }
	// });
    }

    @Override
    public String toString() {
	return StringFactory.graphComputerString(this);
    }

    private static class SynchronizedIterator<V> {

	private final Iterator<V> iterator;

	public SynchronizedIterator(final Iterator<V> iterator) {
	    this.iterator = iterator;
	}

	public synchronized V next() {
	    return this.iterator.hasNext() ? this.iterator.next() : null;
	}
    }

    @Override
    public Features features() {
	return new Features() {

	    @Override
	    public int getMaxWorkers() {
		return Runtime.getRuntime().availableProcessors();
	    }

	    @Override
	    public boolean supportsVertexAddition() {
		return false;
	    }

	    @Override
	    public boolean supportsVertexRemoval() {
		return false;
	    }

	    @Override
	    public boolean supportsVertexPropertyRemoval() {
		return false;
	    }

	    @Override
	    public boolean supportsEdgeAddition() {
		return false;
	    }

	    @Override
	    public boolean supportsEdgeRemoval() {
		return false;
	    }

	    @Override
	    public boolean supportsEdgePropertyAddition() {
		return false;
	    }

	    @Override
	    public boolean supportsEdgePropertyRemoval() {
		return false;
	    }
	};
    }

    @Override
    public GraphComputer vertices(Traversal<Vertex, Vertex> vertexFilter) throws IllegalArgumentException {
	// TODO Auto-generated method stub
	// FIXME
	return null;
    }

    @Override
    public GraphComputer edges(Traversal<Vertex, Edge> edgeFilter) throws IllegalArgumentException {
	// TODO Auto-generated method stub
	// FIXME
	return null;
    }
}
