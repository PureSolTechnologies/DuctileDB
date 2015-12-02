package com.puresoltechnologies.ductiledb.api;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

public interface Graph extends Closeable {

    public Vertex addVertex();

    public Vertex addVertex(Object id);

    public Vertex addVertex(Set<String> labels, Map<String, Object> properties);

    public Vertex getVertex(long vertexId);

    public Vertex getVertex(Object id);

    public Iterable<Vertex> getVertices(String label);

    public Edge addEdge(Vertex startVertex, Vertex targetVertex, String edgeType);

    public Edge addEdge(Object edgeId, Vertex startVertex, Vertex targetVertex, String edgeType);

    public Edge addEdge(Vertex startVertex, Vertex targetVertex, String edgeType, Map<String, Object> properties);

    public Edge getEdge(long edgeId);

    public Edge getEdge(Object edgeId);

    public Iterable<Edge> getEdges(String edgeType);

    public void commit();

    public void rollback();

    public Iterable<Edge> getEdges();

    public Iterable<Edge> getEdges(String propertyKey, Object propertyValue);

    public Iterable<Vertex> getVertices();

    public Iterable<Vertex> getVertices(String propertyKey, Object propertyValue);

    public void removeEdge(Edge edge);

    public void removeVertex(Vertex vertex);

}
