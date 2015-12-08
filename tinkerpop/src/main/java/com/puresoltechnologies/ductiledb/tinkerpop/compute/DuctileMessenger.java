package com.puresoltechnologies.ductiledb.tinkerpop.compute;

import java.util.Iterator;
import java.util.Optional;
import java.util.Queue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.MultiIterator;

public class DuctileMessenger<M> implements Messenger<M> {

    private final Vertex vertex;
    private final DuctileMessageBoard<M> messageBoard;
    private final MessageCombiner<M> combiner;

    public DuctileMessenger(Vertex vertex, DuctileMessageBoard<M> messageBoard, Optional<MessageCombiner<M>> combiner) {
	this.vertex = vertex;
	this.messageBoard = messageBoard;
	this.combiner = combiner.isPresent() ? combiner.get() : null;
    }

    @Override
    public Iterator<M> receiveMessages() {
	MultiIterator<M> multiIterator = new MultiIterator<>();
	for (MessageScope messageScope : messageBoard.getPreviousMessageScopes()) {
	    if (messageScope instanceof MessageScope.Local) {
		@SuppressWarnings("unchecked")
		MessageScope.Local<M> localMessageScope = (MessageScope.Local<M>) messageScope;
		Traversal.Admin<Vertex, Edge> incidentTraversal = DuctileMessenger
			.setVertexStart(localMessageScope.getIncidentTraversal().get().asAdmin(), vertex);
		Direction direction = DuctileMessenger.getDirection(incidentTraversal);
		Edge[] edge = new Edge[1];
		multiIterator.addIterator(//
			StreamSupport
				//
				.stream(Spliterators.spliteratorUnknownSize(
					VertexProgramHelper.reverse(incidentTraversal.asAdmin()),
					Spliterator.IMMUTABLE | Spliterator.SIZED), false)//
				.map(e -> messageBoard.getReceiveMessage((edge[0] = e).vertices(direction).next()))//
				.filter(q -> null != q)//
				.flatMap(Queue::stream)//
				.map(message -> localMessageScope.getEdgeFunction().apply(message, edge[0]))
				.iterator());
	    } else {
		multiIterator.addIterator(Stream.of(vertex).map(messageBoard.getReceiveMessages()::get)
			.filter(q -> null != q).flatMap(Queue::stream).iterator());
	    }
	}
	return multiIterator;
    }

    @Override
    public void sendMessage(MessageScope messageScope, M message) {
	messageBoard.addCurrentMessageScope(messageScope);
	if (messageScope instanceof MessageScope.Local) {
	    addMessage(vertex, message);
	} else {
	    ((MessageScope.Global) messageScope).vertices().forEach(v -> addMessage(v, message));
	}
    }

    private void addMessage(Vertex vertex, M message) {
	messageBoard.getSendMessages().compute(vertex, (v, queue) -> {
	    if (null == queue)
		queue = new ConcurrentLinkedQueue<>();
	    queue.add(null != combiner && !queue.isEmpty() ? combiner.combine(queue.remove(), message) : message);
	    return queue;
	});
    }

    private static <T extends Traversal.Admin<Vertex, Edge>> T setVertexStart(
	    Traversal.Admin<Vertex, Edge> incidentTraversal, Vertex vertex) {
	incidentTraversal.addStep(0, new StartStep<>(incidentTraversal, vertex));
	@SuppressWarnings("unchecked")
	T t = (T) incidentTraversal;
	return t;
    }

    private static Direction getDirection(Traversal.Admin<Vertex, Edge> incidentTraversal) {
	VertexStep<?> step = TraversalHelper.getLastStepOfAssignableClass(VertexStep.class, incidentTraversal).get();
	return step.getDirection();
    }
}
