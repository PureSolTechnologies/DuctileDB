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
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.api.graph.tx.TransactionType;

public class DuctileVertex extends DuctileElement implements Vertex, WrappedVertex<DuctileDBVertex> {

    public static final String LABEL_DELIMINATOR = "::";

    private DuctileDBVertex baseVertex;

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
	DuctileDBEdge baseEdge = getBaseVertex().addEdge(label, ((DuctileVertex) inVertex).getBaseVertex(),
		Collections.emptyMap());
	DuctileEdge edge = new DuctileEdge(baseEdge, graph());
	ElementHelper.attachProperties(edge, keyValues);
	return edge;
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
	    if (value == null) {
		getBaseVertex().removeProperty(key);
	    } else {
		getBaseVertex().setProperty(key, value);
	    }
	    return new DuctileVertexProperty<V>(this, key, value);
	} catch (final IllegalArgumentException iae) {
	    throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
	}
    }

    @Override
    public <V> VertexProperty<V> property(String key) {
	V value = getBaseVertex().getProperty(key);
	if (value == null) {
	    return VertexProperty.empty();
	}
	return new DuctileVertexProperty<V>(this, key, value);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
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
	List<VertexProperty<V>> properties = new ArrayList<>();
	if (propertyKeys.length > 0) {
	    for (String key : propertyKeys) {
		@SuppressWarnings("unchecked")
		V value = (V) getBaseVertex().getProperty(key);
		if (value != null) {
		    properties.add(new DuctileVertexProperty<V>(this, key, value));
		}
	    }
	} else {
	    for (String key : getBaseVertex().getPropertyKeys()) {
		@SuppressWarnings("unchecked")
		V value = (V) getBaseVertex().getProperty(key);
		if (value != null) {
		    properties.add(new DuctileVertexProperty<V>(this, key, value));
		}
	    }
	}
	return properties.iterator();
    }

    @Override
    public DuctileDBVertex getBaseVertex() {
	if ((!baseVertex.getTransaction().isOpen())
		&& (baseVertex.getTransaction().getTransactionType() == TransactionType.THREAD_LOCAL)) {
	    /*
	     * The transaction is closed already, so we reconnect this vertex to
	     * a new base vertex. This is requested by Tinkerpop tests.
	     */
	    baseVertex = graph().getBaseGraph().getVertex(baseVertex.getId());
	}
	return baseVertex;
    }

    @Override
    public String label() {
	graph().tx().readWrite();
	return String.join(LABEL_DELIMINATOR, labels());
    }

    public Set<String> labels() {
	final Set<String> labels = new TreeSet<>();
	for (String label : getBaseVertex().getTypes()) {
	    labels.add(label);
	}
	return Collections.unmodifiableSet(labels);
    }

    @Override
    public boolean equals(final Object object) {
	return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
	return ElementHelper.hashCode(this);
    }

    @Override
    public String toString() {
	return StringFactory.vertexString(this);
    }

}
