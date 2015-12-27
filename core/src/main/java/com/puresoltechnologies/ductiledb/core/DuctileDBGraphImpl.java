package com.puresoltechnologies.ductiledb.core;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Connection;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.tx.DuctileDBTransaction;
import com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema;
import com.puresoltechnologies.ductiledb.core.tx.DuctileDBTransactionImpl;

public class DuctileDBGraphImpl implements DuctileDBGraph {
    private final ThreadLocal<DuctileDBTransaction> transactions = ThreadLocal.withInitial(() -> null);

    private final Connection connection;

    public DuctileDBGraphImpl(Connection connection) throws IOException {
	this.connection = connection;
	new DuctileDBSchema(connection).checkAndCreateEnvironment();
    }

    public final Connection getConnection() {
	return connection;
    }

    @Override
    public void close() throws IOException {
	connection.close();
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
    public DuctileDBEdge addEdge(DuctileDBVertex startVertex, DuctileDBVertex targetVertex, String label,
	    Map<String, Object> properties) {
	return getCurrentTransaction().addEdge(startVertex, targetVertex, label, properties);
    }

    @Override
    public DuctileDBVertex addVertex(Set<String> labels, Map<String, Object> properties) {
	return getCurrentTransaction().addVertex(labels, properties);
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
    public Iterable<DuctileDBEdge> getEdges(String label) {
	return getCurrentTransaction().getEdges(label);
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
    public Iterable<DuctileDBVertex> getVertices(String label) {
	return getCurrentTransaction().getVertices(label);
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
    public void commit() throws IOException {
	DuctileDBTransaction currentTransaction = getCurrentTransaction();
	currentTransaction.commit();
	try {
	    currentTransaction.close();
	} finally {
	    transactions.remove();
	}
    }

    @Override
    public void rollback() throws IOException {
	DuctileDBTransaction currentTransaction = getCurrentTransaction();
	currentTransaction.rollback();
	try {
	    currentTransaction.close();
	} finally {
	    transactions.remove();
	}
    }
}
