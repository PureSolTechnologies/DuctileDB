package com.puresoltechnologies.ductiledb.tinkerpop;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TransactionIT extends AbstractTinkerpopTest {

    private static Graph graph;

    private GraphTraversalSource g;

    @BeforeClass
    public static void setup() {
	graph = getGraph();
    }

    @Before
    public void before() {
	g = graph.traversal();
    }

    @Test
    public void shouldCommitOnCloseWhenConfigured() throws Exception {
	final AtomicReference<Object> oid = new AtomicReference<>();
	final Thread t = new Thread(() -> {
	    final Vertex v1 = graph.addVertex("name", "marko");
	    g.tx().onClose(Transaction.CLOSE_BEHAVIOR.COMMIT);
	    oid.set(v1.id());
	    graph.tx().close();
	});
	t.start();
	t.join();

	final Vertex v2 = graph.vertices(oid.get()).next();
	assertEquals("marko", v2.<String> value("name"));
    }

    @Test
    public void shouldAllowReferenceOfVertexIdOutsideOfOriginalThreadManual() throws Exception {
	g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
	g.tx().open();
	final Vertex v1 = graph.addVertex("name", "stephen");

	final AtomicReference<Object> id = new AtomicReference<>();
	final Thread t = new Thread(() -> {
	    g.tx().open();
	    id.set(v1.id());
	});

	t.start();
	t.join();

	assertEquals(v1.id(), id.get());

	g.tx().rollback();
    }

}
