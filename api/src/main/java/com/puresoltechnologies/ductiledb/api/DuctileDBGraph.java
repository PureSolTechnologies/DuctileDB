package com.puresoltechnologies.ductiledb.api;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

public interface DuctileDBGraph extends Closeable {

    public DuctileDBVertex addVertex();

    public DuctileDBVertex addVertex(Object id);

    public DuctileDBVertex addVertex(Set<String> labels, Map<String, Object> properties);

    public DuctileDBVertex getVertex(long vertexId);

    public Iterable<DuctileDBVertex> getVertices(String label);

    public DuctileDBEdge addEdge(DuctileDBVertex startVertex, DuctileDBVertex targetVertex, String edgeType);

    public DuctileDBEdge addEdge(Object edgeId, DuctileDBVertex startVertex, DuctileDBVertex targetVertex,
	    String edgeType);

    public DuctileDBEdge addEdge(DuctileDBVertex startVertex, DuctileDBVertex targetVertex, String edgeType,
	    Map<String, Object> properties);

    public DuctileDBEdge getEdge(long edgeId);

    public DuctileDBEdge getEdge(Object edgeId);

    public Iterable<DuctileDBEdge> getEdges(String edgeType);

    public void commit();

    public void rollback();

    public Iterable<DuctileDBEdge> getEdges();

    public Iterable<DuctileDBEdge> getEdges(String propertyKey, Object propertyValue);

    public Iterable<DuctileDBVertex> getVertices();

    public Iterable<DuctileDBVertex> getVertices(String propertyKey, Object propertyValue);

    public void removeEdge(DuctileDBEdge edge);

    public void removeVertex(DuctileDBVertex vertex);

}
