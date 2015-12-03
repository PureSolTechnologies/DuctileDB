package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;

public class DuctileVertex extends DuctileElement implements Vertex, WrappedVertex<DuctileDBVertex> {

    public static final String LABEL_DELIMINATOR = "::";

    private final DuctileDBVertex baseVertex;

    public DuctileVertex(DuctileDBVertex baseVertex, DuctileGraph graph) {
	super(baseVertex.getId(), graph);
	this.baseVertex = baseVertex;
    }

    @Override
    public <V> DuctileVertexProperty<V> property(String key, V value) {
	return property(VertexProperty.Cardinality.single, key, value);
    }

    @Override
    public DuctileEdge addEdge(String label, Vertex inVertex, Object... keyValues) {
	if (null == inVertex) {
	    throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
	}
	ElementHelper.validateLabel(label);
	ElementHelper.legalPropertyKeyValueArray(keyValues);
	if (ElementHelper.getIdValue(keyValues).isPresent()) {
	    throw Edge.Exceptions.userSuppliedIdsNotSupported();
	}
	graph().tx().readWrite();
	DuctileDBEdge edge = baseVertex.addEdge(label, ((DuctileVertex) inVertex).getBaseVertex());
	DuctileEdge ductileEdge = new DuctileEdge(edge, graph());
	ElementHelper.attachProperties(ductileEdge, keyValues);
	return ductileEdge;
    }

    @Override
    public <V> DuctileVertexProperty<V> property(Cardinality cardinality, String key, V value, Object... keyValues) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public DuctileDBVertex getBaseVertex() {
	return baseVertex;
    }

    @Override
    public String label() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void remove() {
	graph().tx().readWrite();
	graph().getBaseGraph().removeVertex(baseVertex);
    }

}
