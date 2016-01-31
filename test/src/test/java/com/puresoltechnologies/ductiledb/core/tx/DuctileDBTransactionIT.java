package com.puresoltechnologies.ductiledb.core.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.configuration.BaseConfiguration;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBInvalidPropertyKeyException;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBInvalidTypeNameException;
import com.puresoltechnologies.ductiledb.api.tx.DuctileDBTransaction;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBGraphTest;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphFactory;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.DuctileDBTestHelper;
import com.puresoltechnologies.ductiledb.core.schema.DuctileDBHealthCheck;

/**
 * This intergrationt tests check the correct behavior of transactions.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBTransactionIT extends AbstractDuctileDBGraphTest {

    private static DuctileDBGraphImpl graph;

    @BeforeClass
    public static void initializeGraph() {
	graph = getGraph();
    }

    @After
    public void checkGraph() throws IOException {
	DuctileDBHealthCheck.runCheck(graph);
    }

    @Test
    public void shouldAllowJustCommitOnlyWithAutoTransaction() {
	// not expecting any exceptions here
	graph.getCurrentTransaction().commit();
    }

    @Test
    public void shouldAllowJustRollbackOnlyWithAutoTransaction() {
	// not expecting any exceptions here
	graph.getCurrentTransaction().rollback();
    }

    @Test
    public void shouldAllowAutoTransactionToWorkWithoutMutationByDefault() {
	// expecting no exceptions to be thrown here
	graph.getCurrentTransaction().commit();
	graph.getCurrentTransaction().rollback();
	graph.getCurrentTransaction().commit();
    }

    @Test
    public void shouldNotifyTransactionListenersOnCommitSuccess() {
	final AtomicInteger count = new AtomicInteger(0);
	graph.getCurrentTransaction().addTransactionListener(s -> {
	    if (s == DuctileDBTransaction.Status.COMMIT)
		count.incrementAndGet();
	});
	graph.getCurrentTransaction().commit();

	assertEquals(1, count.get());
    }

    @Test
    public void shouldNotifyTransactionListenersInSameThreadOnlyOnCommitSuccess() throws Exception {
	final AtomicInteger count = new AtomicInteger(0);
	graph.getCurrentTransaction().addTransactionListener(s -> {
	    if (s == DuctileDBTransaction.Status.COMMIT)
		count.incrementAndGet();
	});

	final Thread t = new Thread(() -> graph.getCurrentTransaction().commit());
	t.start();
	t.join();

	assertEquals(0, count.get());
    }

    @Test
    public void shouldNotifyTransactionListenersOnRollbackSuccess() {
	final AtomicInteger count = new AtomicInteger(0);
	graph.getCurrentTransaction().addTransactionListener(s -> {
	    if (s == DuctileDBTransaction.Status.ROLLBACK)
		count.incrementAndGet();
	});
	graph.getCurrentTransaction().rollback();

	assertEquals(1, count.get());
    }

    @Test
    public void shouldNotifyTransactionListenersInSameThreadOnlyOnRollbackSuccess() throws Exception {
	final AtomicInteger count = new AtomicInteger(0);
	graph.getCurrentTransaction().addTransactionListener(s -> {
	    if (s == DuctileDBTransaction.Status.ROLLBACK)
		count.incrementAndGet();
	});

	final Thread t = new Thread(() -> graph.getCurrentTransaction().rollback());
	t.start();
	t.join();

	assertEquals(0, count.get());
    }

    @Test
    public void shouldCommitElementAutoTransactionByDefault() {
	final DuctileDBVertex v1 = graph.addVertex();
	final DuctileDBEdge e1 = v1.addEdge("l", v1, Collections.emptyMap());
	assertVertexEdgeCounts(1, 1);
	assertEquals(v1.getId(), graph.getVertex(v1.getId()).getId());
	assertEquals(e1.getId(), graph.getEdge(e1.getId()).getId());
	graph.getCurrentTransaction().commit();
	assertVertexEdgeCounts(1, 1);
	assertEquals(v1.getId(), graph.getVertex(v1.getId()).getId());
	assertEquals(e1.getId(), graph.getEdge(e1.getId()).getId());

	graph.getVertex(v1.getId()).remove();
	assertVertexEdgeCounts(0, 0);
	graph.getCurrentTransaction().rollback();
	assertVertexEdgeCounts(1, 1);

	graph.getVertex(v1.getId()).remove();
	assertVertexEdgeCounts(0, 0);
	graph.getCurrentTransaction().commit();
	assertVertexEdgeCounts(0, 0);
    }

    @Test
    public void shouldRollbackElementAutoTransactionByDefault() {
	final DuctileDBVertex v1 = graph.addVertex();
	final DuctileDBEdge e1 = v1.addEdge("l", v1, Collections.emptyMap());
	assertVertexEdgeCounts(1, 1);
	assertEquals(v1.getId(), graph.getVertex(v1.getId()).getId());
	assertEquals(e1.getId(), graph.getEdge(e1.getId()).getId());
	graph.getCurrentTransaction().rollback();
	assertVertexEdgeCounts(0, 0);
    }

    @Test
    public void shouldCommitPropertyAutoTransactionByDefault() {
	final DuctileDBVertex v1 = graph.addVertex();
	final DuctileDBEdge e1 = v1.addEdge("l", v1, Collections.emptyMap());
	graph.getCurrentTransaction().commit();
	assertVertexEdgeCounts(1, 1);
	assertEquals(v1.getId(), graph.getVertex(v1.getId()).getId());
	assertEquals(e1.getId(), graph.getEdge(e1.getId()).getId());

	v1.setProperty("name", "marko");
	assertEquals("marko", v1.<String> getProperty("name"));
	assertEquals("marko", graph.getVertex(v1.getId()).<String> getProperty("name"));
	graph.getCurrentTransaction().commit();

	assertEquals("marko", v1.<String> getProperty("name"));
	assertEquals("marko", graph.getVertex(v1.getId()).<String> getProperty("name"));

	v1.setProperty("name", "stephen");

	assertEquals("stephen", v1.<String> getProperty("name"));
	assertEquals("stephen", graph.getVertex(v1.getId()).<String> getProperty("name"));

	graph.getCurrentTransaction().commit();

	assertEquals("stephen", v1.<String> getProperty("name"));
	assertEquals("stephen", graph.getVertex(v1.getId()).<String> getProperty("name"));

	e1.setProperty("name", "xxx");

	assertEquals("xxx", e1.<String> getProperty("name"));
	assertEquals("xxx", graph.getEdge(e1.getId()).<String> getProperty("name"));

	graph.getCurrentTransaction().commit();

	assertEquals("xxx", e1.<String> getProperty("name"));
	assertEquals("xxx", graph.getEdge(e1.getId()).<String> getProperty("name"));

	assertVertexEdgeCounts(1, 1);
	assertEquals(v1.getId(), graph.getVertex(v1.getId()).getId());
	assertEquals(e1.getId(), graph.getEdge(e1.getId()).getId());
    }

    @Test
    public void shouldRollbackPropertyAutoTransactionByDefault() throws IOException {
	final DuctileDBVertex v1 = graph.addVertex(new HashSet<>(), toMap("name", "marko"));
	final DuctileDBEdge e1 = v1.addEdge("l", v1, toMap("name", "xxx"));
	assertVertexEdgeCounts(1, 1);
	assertEquals(v1.getId(), graph.getVertex(v1.getId()).getId());
	assertEquals(e1.getId(), graph.getEdge(e1.getId()).getId());
	assertEquals("marko", v1.<String> getProperty("name"));
	assertEquals("xxx", e1.<String> getProperty("name"));
	graph.commit();

	assertEquals("marko", v1.<String> getProperty("name"));
	assertEquals("marko", graph.getVertex(v1.getId()).<String> getProperty("name"));

	v1.setProperty("name", "stephen");

	assertEquals("stephen", v1.<String> getProperty("name"));
	assertEquals("stephen", graph.getVertex(v1.getId()).<String> getProperty("name"));

	graph.getCurrentTransaction().rollback();

	assertEquals("marko", v1.<String> getProperty("name"));
	assertEquals("marko", graph.getVertex(v1.getId()).<String> getProperty("name"));

	e1.setProperty("name", "yyy");

	assertEquals("yyy", e1.<String> getProperty("name"));
	assertEquals("yyy", graph.getEdge(e1.getId()).<String> getProperty("name"));

	graph.getCurrentTransaction().rollback();

	assertEquals("xxx", e1.<String> getProperty("name"));
	assertEquals("xxx", graph.getEdge(e1.getId()).<String> getProperty("name"));

	assertVertexEdgeCounts(1, 1);
    }

    @Test
    public void shouldRollbackOnCloseByDefault() throws Exception {
	final AtomicReference<Long> oid = new AtomicReference<>();
	final AtomicReference<DuctileDBVertex> vid = new AtomicReference<>();
	final Thread t = new Thread(() -> {
	    vid.set(graph.addVertex(new HashSet<>(), toMap("name", "stephen")));
	    try {
		graph.commit();
		try (DuctileDBTransaction ignored = graph.getCurrentTransaction()) {
		    final DuctileDBVertex v1 = graph.addVertex(new HashSet<>(), toMap("name", "marko"));
		    oid.set(v1.getId());
		}
	    } catch (Exception e) {
		throw new RuntimeException(e);
	    }
	});
	t.start();
	t.join();

	// this was committed
	assertNotNull(graph.getVertex(vid.get().getId()));

	// this was not
	assertNull("Vertex should not be found as close behavior was set to rollback", graph.getVertex(oid.get()));
    }

    @Test
    public void shouldExecuteWithCompetingThreads() {
	int totalThreads = 250;
	final AtomicInteger vertices = new AtomicInteger(0);
	final AtomicInteger edges = new AtomicInteger(0);
	final AtomicInteger completedThreads = new AtomicInteger(0);
	for (int i = 0; i < totalThreads; i++) {
	    new Thread() {
		@Override
		public void run() {
		    final Random random = new Random();
		    if (random.nextBoolean()) {
			final DuctileDBVertex a = graph.addVertex();
			final DuctileDBVertex b = graph.addVertex();
			final DuctileDBEdge e = a.addEdge("friend", b, Collections.emptyMap());

			vertices.getAndAdd(2);
			a.setProperty("test", this.getId());
			b.setProperty("blah", random.nextDouble());
			e.setProperty("bloop", random.nextInt());
			edges.getAndAdd(1);
			graph.commit();
		    } else {
			final DuctileDBVertex a = graph.addVertex();
			final DuctileDBVertex b = graph.addVertex();
			final DuctileDBEdge e = a.addEdge("friend", b, Collections.emptyMap());

			a.setProperty("test", this.getId());
			b.setProperty("blah", random.nextDouble());
			e.setProperty("bloop", random.nextInt());

			if (random.nextBoolean()) {
			    graph.commit();
			    vertices.getAndAdd(2);
			    edges.getAndAdd(1);
			} else {
			    graph.rollback();
			}
		    }
		    completedThreads.getAndAdd(1);
		}
	    }.start();
	}

	while (completedThreads.get() < totalThreads) {
	}

	assertEquals(completedThreads.get(), 250);
	assertVertexEdgeCounts(vertices.get(), edges.get());
    }

    @Test
    public void shouldExecuteCompetingThreadsOnMultipleDbInstances() throws Exception {
	// the idea behind this test is to simulate a gremlin-server environment
	// where two graphs of the same type
	// are being mutated by multiple threads. originally replicated a bug
	// that was part of OrientDB.

	final DuctileDBGraph g1 = DuctileDBGraphFactory.createGraph(new BaseConfiguration());

	final Thread threadModFirstGraph = new Thread() {
	    @Override
	    public void run() {
		graph.addVertex();
		graph.getCurrentTransaction().commit();
	    }
	};

	threadModFirstGraph.start();
	threadModFirstGraph.join();

	final Thread threadReadBothGraphs = new Thread() {
	    @Override
	    public void run() {
		final long gCounter = DuctileDBTestHelper.count(graph.getVertices());
		assertEquals(1l, gCounter);

		final long g1Counter = DuctileDBTestHelper.count(g1.getVertices());
		assertEquals(0l, g1Counter);
	    }
	};

	threadReadBothGraphs.start();
	threadReadBothGraphs.join();
    }

    @Test
    public void shouldSupportTransactionIsolationCommitCheck() throws Exception {
	// the purpose of this test is to simulate gremlin server access to a
	// graph instance, where one thread modifies
	// the graph and a separate thread cannot affect the transaction of the
	// first
	final CountDownLatch latchCommittedInOtherThread = new CountDownLatch(1);
	final CountDownLatch latchCommitInOtherThread = new CountDownLatch(1);

	final AtomicBoolean noVerticesInFirstThread = new AtomicBoolean(false);

	// this thread starts a transaction then waits while the second thread
	// tries to commit it.
	final Thread threadTxStarter = new Thread() {
	    @Override
	    public void run() {
		graph.addVertex();
		latchCommitInOtherThread.countDown();

		try {
		    latchCommittedInOtherThread.await();
		} catch (InterruptedException ie) {
		    throw new RuntimeException(ie);
		}

		graph.rollback();

		// there should be no vertices here
		noVerticesInFirstThread.set(!graph.getVertices().iterator().hasNext());
	    }
	};

	threadTxStarter.start();

	// this thread tries to commit the transaction started in the first
	// thread above.
	final Thread threadTryCommitTx = new Thread() {
	    @Override
	    public void run() {
		try {
		    latchCommitInOtherThread.await();
		} catch (InterruptedException ie) {
		    throw new RuntimeException(ie);
		}

		// try to commit the other transaction
		graph.commit();

		latchCommittedInOtherThread.countDown();
	    }
	};

	threadTryCommitTx.start();

	threadTxStarter.join();
	threadTryCommitTx.join();

	assertTrue(noVerticesInFirstThread.get());
	assertVertexEdgeCounts(0, 0);
    }

    @Test
    public void shouldCountVerticesEdgesOnPreTransactionCommit() throws IOException {
	// see a more complex version of this test at
	// GraphTest.shouldProperlyCountVerticesAndEdgesOnAddRemove()
	DuctileDBVertex v1 = graph.addVertex();
	graph.commit();

	assertVertexEdgeCounts(1, 0);

	final DuctileDBVertex v2 = graph.addVertex();
	v1 = graph.getVertex(v1.getId());
	v1.addEdge("friend", v2, Collections.emptyMap());

	assertVertexEdgeCounts(2, 1);

	graph.commit();

	assertVertexEdgeCounts(2, 1);
    }

    @Test
    public void shouldAllowReferenceOfVertexOutsideOfOriginalTransactionalContextAuto() {
	final DuctileDBVertex v1 = graph.addVertex(new HashSet<>(), toMap("name", "stephen"));
	graph.getCurrentTransaction().commit();

	assertEquals("stephen", v1.getProperty("name"));

	graph.getCurrentTransaction().rollback();
	assertEquals("stephen", v1.getProperty("name"));

    }

    @Test
    public void shouldAllowReferenceOfEdgeOutsideOfOriginalTransactionalContextAuto() {
	final DuctileDBVertex v1 = graph.addVertex();
	final DuctileDBEdge e = v1.addEdge("self", v1, toMap("weight", 0.5d));
	graph.getCurrentTransaction().commit();

	assertEquals(0.5d, e.getProperty("weight"), 0.00001d);

	graph.getCurrentTransaction().rollback();
	assertEquals(0.5d, e.getProperty("weight"), 0.00001d);
    }

    @Test
    public void shouldAllowReferenceOfVertexIdOutsideOfOriginalThreadAuto() throws Exception {
	final DuctileDBVertex v1 = graph.addVertex(new HashSet<>(), toMap("name", "stephen"));

	final AtomicReference<Object> id = new AtomicReference<>();
	final Thread t = new Thread(() -> id.set(v1.getId()));
	t.start();
	t.join();

	assertEquals(v1.getId(), id.get());

	graph.getCurrentTransaction().rollback();
    }

    @Test
    public void shouldAllowReferenceOfEdgeIdOutsideOfOriginalThreadAuto() throws Exception {
	final DuctileDBVertex v1 = graph.addVertex();
	final DuctileDBEdge e = v1.addEdge("self", v1, toMap("weight", 0.5d));

	final AtomicReference<Object> id = new AtomicReference<>();
	final Thread t = new Thread(() -> id.set(e.getId()));
	t.start();
	t.join();

	assertEquals(e.getId(), id.get());

	graph.getCurrentTransaction().rollback();
    }

    @Test
    public void shouldNotShareTransactionCloseConsumersAcrossThreads() throws InterruptedException {
	final CountDownLatch latch = new CountDownLatch(1);

	final Thread manualThread = new Thread(() -> {
	    try {
		latch.await();
	    } catch (InterruptedException ie) {
		throw new RuntimeException(ie);
	    }
	});

	manualThread.start();

	final Thread autoThread = new Thread(() -> {
	    try {
		latch.countDown();
		graph.addVertex();
		graph.rollback();
	    } catch (Exception e) {
		throw new RuntimeException(e);
	    }
	});

	autoThread.start();

	manualThread.join();
	autoThread.join();

	assertEquals("Graph should be empty. autoThread transaction.onClose() should be ROLLBACK (default)", 0,
		DuctileDBTestHelper.count(graph.getVertices()));
    }

    @Test
    public void testValidTypes() {
	Set<String> types = new HashSet<>();
	types.add("1234567890");
	types.add("abcdefghijklmnopqrstuvwxyz");
	types.add("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
	types.add("1._-");
	try {
	    graph.addVertex(types, new HashMap<>());
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBInvalidTypeNameException.class)
    public void testInvalidType1() {
	Set<String> types = new HashSet<>();
	types.add(".123");
	try {
	    graph.addVertex(types, new HashMap<>());
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBInvalidTypeNameException.class)
    public void testInvalidType2() {
	Set<String> types = new HashSet<>();
	types.add("_123");
	try {
	    graph.addVertex(types, new HashMap<>());
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBInvalidTypeNameException.class)
    public void testInvalidType3() {
	Set<String> types = new HashSet<>();
	types.add("-123");
	try {
	    graph.addVertex(types, new HashMap<>());
	} finally {
	    graph.rollback();
	}
    }

    @Test
    public void testValidPropertyNames() {
	Set<String> types = new HashSet<>();
	types.add("type");
	HashMap<String, Object> properties = new HashMap<>();
	properties.put("1234567890", "value");
	properties.put("abcdefghijklmnopqrstuvwxyz", "value");
	properties.put("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "value");
	properties.put("1._-", "value");
	try {
	    graph.addVertex(types, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBInvalidPropertyKeyException.class)
    public void testInvalidPropertyName() {
	Set<String> types = new HashSet<>();
	types.add("type");
	HashMap<String, Object> properties = new HashMap<>();
	properties.put(".123", "value");
	try {
	    graph.addVertex(types, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBInvalidPropertyKeyException.class)
    public void testInvalidPropertyName2() {
	Set<String> types = new HashSet<>();
	types.add("type");
	HashMap<String, Object> properties = new HashMap<>();
	properties.put("_123", "value");
	try {
	    graph.addVertex(types, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBInvalidPropertyKeyException.class)
    public void testInvalidPropertyName3() {
	Set<String> types = new HashSet<>();
	types.add("type");
	HashMap<String, Object> properties = new HashMap<>();
	properties.put("-123", "value");
	try {
	    graph.addVertex(types, properties);
	} finally {
	    graph.rollback();
	}
    }
}
