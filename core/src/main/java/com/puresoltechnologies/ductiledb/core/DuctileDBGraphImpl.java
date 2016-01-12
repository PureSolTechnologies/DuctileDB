package com.puresoltechnologies.ductiledb.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBSchemaManager;
import com.puresoltechnologies.ductiledb.api.tx.DuctileDBCommitException;
import com.puresoltechnologies.ductiledb.api.tx.DuctileDBRollbackException;
import com.puresoltechnologies.ductiledb.api.tx.DuctileDBTransaction;
import com.puresoltechnologies.ductiledb.core.manager.DuctileDBGraphManagerImpl;
import com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema;
import com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchemaManagerImpl;
import com.puresoltechnologies.ductiledb.core.schema.HBaseSchema;
import com.puresoltechnologies.ductiledb.core.tx.DuctileDBTransactionImpl;

public class DuctileDBGraphImpl implements DuctileDBGraph {

    private static Logger logger = LoggerFactory.getLogger(DuctileDBGraphImpl.class);

    private final ThreadLocal<DuctileDBTransaction> transactions = ThreadLocal.withInitial(() -> null);
    private final List<Consumer<Status>> transactionListeners = new ArrayList<>();
    private final DuctileDBSchema schema;

    private final Connection connection;

    public DuctileDBGraphImpl(Connection connection) throws IOException {
	this.connection = connection;
	new HBaseSchema(connection).checkAndCreateEnvironment();
	schema = new DuctileDBSchema(this);
    }

    public final Connection getConnection() {
	return connection;
    }

    @Override
    public void close() throws IOException {
	if (connection.isClosed()) {
	    throw new IllegalStateException("Connection was already closed.");
	}
	logger.info("Closes connection '" + connection.toString() + "'...");
	connection.close();
	logger.info("Connection '" + connection.toString() + "' closed.");
    }

    @Override
    public DuctileDBTransaction createTransaction() {
	return new DuctileDBTransactionImpl(this);
    }

    public DuctileDBTransaction getCurrentTransaction() {
	DuctileDBTransaction transaction = transactions.get();
	if (transaction == null) {
	    transaction = new DuctileDBTransactionImpl(this);
	    transactions.set(transaction);
	}
	return transaction;
    }

    @Override
    public DuctileDBGraphManager createGraphManager() {
	return new DuctileDBGraphManagerImpl(this);
    }

    @Override
    public DuctileDBSchemaManager createSchemaManager() {
	return new DuctileDBSchemaManagerImpl(this);
    }

    public DuctileDBSchema getSchema() {
	return schema;
    }

    @Override
    public DuctileDBEdge addEdge(DuctileDBVertex startVertex, DuctileDBVertex targetVertex, String type,
	    Map<String, Object> properties) {
	return getCurrentTransaction().addEdge(startVertex, targetVertex, type, properties);
    }

    @Override
    public DuctileDBVertex addVertex(Set<String> types, Map<String, Object> properties) {
	return getCurrentTransaction().addVertex(types, properties);
    }

    @Override
    public DuctileDBEdge getEdge(long edgeId) {
	return getCurrentTransaction().getEdge(edgeId);
    }

    @Override
    public Iterable<DuctileDBEdge> getEdges() {
	return getCurrentTransaction().getEdges();
    }

    @Override
    public Iterable<DuctileDBEdge> getEdges(String propertyKey, Object propertyValue) {
	return getCurrentTransaction().getEdges(propertyKey, propertyValue);
    }

    @Override
    public Iterable<DuctileDBEdge> getEdges(String type) {
	return getCurrentTransaction().getEdges(type);
    }

    @Override
    public DuctileDBVertex getVertex(long vertexId) {
	return getCurrentTransaction().getVertex(vertexId);
    }

    @Override
    public Iterable<DuctileDBVertex> getVertices() {
	return getCurrentTransaction().getVertices();
    }

    @Override
    public Iterable<DuctileDBVertex> getVertices(String propertyKey, Object propertyValue) {
	return getCurrentTransaction().getVertices(propertyKey, propertyValue);
    }

    @Override
    public Iterable<DuctileDBVertex> getVertices(String type) {
	return getCurrentTransaction().getVertices(type);
    }

    @Override
    public void removeEdge(DuctileDBEdge edge) {
	getCurrentTransaction().removeEdge(edge);
    }

    @Override
    public void removeVertex(DuctileDBVertex vertex) {
	getCurrentTransaction().removeVertex(vertex);
    }

    @Override
    public void commit() {
	DuctileDBTransaction currentTransaction = getCurrentTransaction();
	currentTransaction.commit();
	try {
	    currentTransaction.close();
	} catch (IOException e) {
	    throw new DuctileDBCommitException(e);
	} finally {
	    transactions.remove();
	    fireOnCommit();
	}
    }

    @Override
    public void rollback() {
	DuctileDBTransaction currentTransaction = getCurrentTransaction();
	currentTransaction.rollback();
	try {
	    currentTransaction.close();
	} catch (IOException e) {
	    throw new DuctileDBRollbackException(e);
	} finally {
	    transactions.remove();
	    fireOnRollback();
	}
    }

    @Override
    public boolean isOpen() {
	/*
	 * On graph level, there is always a transaction available. If not, one
	 * is created instantaneously.
	 */
	return true;
    }

    @Override
    public void addTransactionListener(Consumer<Status> listener) {
	transactionListeners.add(listener);
    }

    @Override
    public void removeTransactionListener(Consumer<Status> listener) {
	transactionListeners.remove(listener);
    }

    @Override
    public void clearTransactionListeners() {
	transactionListeners.clear();
    }

    private void fireOnCommit() {
	transactionListeners.forEach(c -> c.accept(Status.COMMIT));
    }

    private void fireOnRollback() {
	transactionListeners.forEach(c -> c.accept(Status.ROLLBACK));
    }
}
