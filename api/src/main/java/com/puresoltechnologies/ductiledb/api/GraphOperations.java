package com.puresoltechnologies.ductiledb.api;

import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.tx.DuctileDBTransaction;

/**
 * This interface defines all public operations on a graph. The interface is
 * used to define the {@link DuctileDBGraph} and also the transaction
 * {@link DuctileDBTransaction}.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface GraphOperations {

    public DuctileDBVertex addVertex();

    public DuctileDBVertex addVertex(Set<String> labels, Map<String, Object> properties);

    public DuctileDBVertex getVertex(long vertexId);

    public void removeVertex(DuctileDBVertex vertex);

    public Iterable<DuctileDBVertex> getVertices();

    public Iterable<DuctileDBVertex> getVertices(String label);

    public Iterable<DuctileDBVertex> getVertices(String propertyKey, Object propertyValue);

    public DuctileDBEdge addEdge(DuctileDBVertex startVertex, DuctileDBVertex targetVertex, String edgeType);

    public DuctileDBEdge addEdge(DuctileDBVertex startVertex, DuctileDBVertex targetVertex, String edgeType,
	    Map<String, Object> properties);

    public DuctileDBEdge getEdge(long edgeId);

    public void removeEdge(DuctileDBEdge edge);

    public Iterable<DuctileDBEdge> getEdges();

    public Iterable<DuctileDBEdge> getEdges(String edgeType);

    public Iterable<DuctileDBEdge> getEdges(String propertyKey, Object propertyValue);

    public void addLabel(DuctileDBVertex vertex, String label);

    public void removeLabel(DuctileDBVertex vertex, String label);

    public void setProperty(DuctileDBVertex vertex, String key, Object value);

    public void removeProperty(DuctileDBVertex vertex, String key);

    public void setProperty(DuctileDBEdge edge, String key, Object value);

    public void removeProperty(DuctileDBEdge edge, String key);
}
