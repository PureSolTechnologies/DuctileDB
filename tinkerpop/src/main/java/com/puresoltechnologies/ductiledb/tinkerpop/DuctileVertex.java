package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

public class DuctileVertex extends DuctileElement implements Vertex {

    @Override
    public <V> DuctileVertexProperty<V> property(String key, V value) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public <V> VertexProperty<V> property(Cardinality cardinality, String key, V value, Object... keyValues) {
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

}
