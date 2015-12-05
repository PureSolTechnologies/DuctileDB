package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;

public class DuctileVertex extends DuctileElement implements Vertex, WrappedVertex<DuctileDBVertex> {

    public static final String LABEL_DELIMINATOR = "::";

    private final DuctileDBVertex baseVertex;

    public DuctileVertex(DuctileDBVertex baseVertex, DuctileGraph graph) {
	super(baseVertex, graph);
	this.baseVertex = baseVertex;
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
    public <V> DuctileVertexProperty<V> property(String key, V value) {
	return property(VertexProperty.Cardinality.single, key, value);
    }

    @Override
    public void remove() {
	graph().tx().readWrite();
	graph().getBaseGraph().removeVertex(baseVertex);
    }

    @Override
    public <V> DuctileVertexProperty<V> property(Cardinality cardinality, String key, V value, Object... keyValues) {
	if (cardinality != VertexProperty.Cardinality.single) {
	    throw VertexProperty.Exceptions.multiPropertiesNotSupported();
	}
	if (keyValues.length > 0) {
	    throw VertexProperty.Exceptions.metaPropertiesNotSupported();
	}
	ElementHelper.validateProperty(key, value);
	if (ElementHelper.getIdValue(keyValues).isPresent())
	    throw Vertex.Exceptions.userSuppliedIdsNotSupported();
	graph().tx().readWrite();
	try {
	    baseVertex.setProperty(key, value);
	    return new DuctileVertexProperty<V>(this, key, value);
	} catch (final IllegalArgumentException iae) {
	    throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
	}
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
	graph().tx().readWrite();
	Iterable<DuctileDBEdge> edges;
	switch (direction) {
	case IN:
	    edges = getBaseVertex().getEdges(EdgeDirection.IN, edgeLabels);
	    break;
	case OUT:
	    edges = getBaseVertex().getEdges(EdgeDirection.OUT, edgeLabels);
	    break;
	case BOTH:
	    edges = getBaseVertex().getEdges(EdgeDirection.BOTH, edgeLabels);
	    break;
	default:
	    throw new IllegalArgumentException("Direction '" + direction + "' not supported.");
	}
	List<Edge> resultEdges = new ArrayList<>();
	for (DuctileDBEdge edge : edges) {
	    resultEdges.add(new DuctileEdge(edge, graph()));
	}
	return resultEdges.iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
	graph().tx().readWrite();
	Iterable<DuctileDBVertex> vertices;
	switch (direction) {
	case IN:
	    vertices = getBaseVertex().getVertices(EdgeDirection.IN, edgeLabels);
	    break;
	case OUT:
	    vertices = getBaseVertex().getVertices(EdgeDirection.OUT, edgeLabels);
	    break;
	case BOTH:
	    vertices = getBaseVertex().getVertices(EdgeDirection.BOTH, edgeLabels);
	    break;
	default:
	    throw new IllegalArgumentException("Direction '" + direction + "' not supported.");
	}
	List<Vertex> resultVertices = new ArrayList<>();
	for (DuctileDBVertex vertex : vertices) {
	    resultVertices.add(new DuctileVertex(vertex, graph()));
	}
	return resultVertices.iterator();
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
	graph().tx().readWrite();
	return String.join(LABEL_DELIMINATOR, labels());
    }

    public Set<String> labels() {
	graph().tx().readWrite();
	final Set<String> labels = new TreeSet<>();
	for (String label : getBaseVertex().getLabels()) {
	    labels.add(label);
	}
	return Collections.unmodifiableSet(labels);
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((baseVertex == null) ? 0 : baseVertex.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	DuctileVertex other = (DuctileVertex) obj;
	if (baseVertex == null) {
	    if (other.baseVertex != null)
		return false;
	} else if (!baseVertex.equals(other.baseVertex))
	    return false;
	return true;
    }

}
