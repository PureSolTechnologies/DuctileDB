package com.puresoltechnologies.ductiledb.tinkerpop.compute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertex;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertexProperty;

public class DuctileGraphComputerView {

    private final Map<Element, Map<String, List<VertexProperty<?>>>> computeProperties = new ConcurrentHashMap<>();
    private final DuctileGraph graph;
    protected final Set<String> computeKeys;

    public DuctileGraphComputerView(DuctileGraph graph, Set<String> computeKeys) {
	this.graph = graph;
	this.computeKeys = computeKeys;
    }

    public <V> Property<V> addProperty(DuctileVertex vertex, String key, V value) {
	ElementHelper.validateProperty(key, value);
	if (!isComputeKey(key)) {
	    throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
	}
	DuctileVertexProperty<V> property = new DuctileVertexProperty<V>(vertex, key, value) {
	    @Override
	    public void remove() {
		removeProperty(vertex, key, this);
	    }
	};
	addValue(vertex, key, property);
	return property;
    }

    public List<VertexProperty<?>> getProperty(DuctileVertex vertex, String key) {
	if (isComputeKey(key)) {
	    return getValue(vertex, key);
	} else {
	    return DuctileHelper.getProperties(vertex).getOrDefault(key, Collections.emptyList());
	}
    }

    public List<Property<?>> getProperties(DuctileVertex vertex) {
	@SuppressWarnings("rawtypes")
	Stream<Property> a = DuctileHelper.getProperties(vertex).values().stream().flatMap(list -> list.stream());
	@SuppressWarnings("rawtypes")
	Stream<Property> b = computeProperties.containsKey(vertex)
		? computeProperties.get(vertex).values().stream().flatMap(list -> list.stream()) : Stream.empty();
	return Stream.concat(a, b).collect(Collectors.toList());
    }

    public void removeProperty(DuctileVertex vertex, String key, DuctileVertexProperty<?> property) {
	if (isComputeKey(key)) {
	    removeValue(vertex, key, property);
	} else {
	    throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
	}
    }

    public Graph processResultGraphPersist(GraphComputer.ResultGraph resultGraph, GraphComputer.Persist persist) {
	if (GraphComputer.Persist.NOTHING == persist) {
	    if (GraphComputer.ResultGraph.ORIGINAL == resultGraph)
		return graph;
	    else
		return EmptyGraph.instance();
	} else if (GraphComputer.Persist.VERTEX_PROPERTIES == persist) {
	    if (GraphComputer.ResultGraph.ORIGINAL == resultGraph) {
		addPropertiesToOriginalGraph();
		return graph;
	    } else {
		TinkerGraph newGraph = TinkerGraph.open();
		graph.vertices().forEachRemaining(vertex -> {
		    Vertex newVertex = newGraph.addVertex(T.id, vertex.id(), T.label, vertex.label());
		    vertex.properties().forEachRemaining(vertexProperty -> {
			VertexProperty<?> newVertexProperty = newVertex.property(VertexProperty.Cardinality.list,
				vertexProperty.key(), vertexProperty.value(), T.id, vertexProperty.id());
			vertexProperty.properties().forEachRemaining(property -> {
			    newVertexProperty.property(property.key(), property.value());
			});
		    });
		});
		return newGraph;
	    }
	} else { // Persist.EDGES
	    if (GraphComputer.ResultGraph.ORIGINAL == resultGraph) {
		addPropertiesToOriginalGraph();
		return graph;
	    } else {
		TinkerGraph newGraph = TinkerGraph.open();
		graph.vertices().forEachRemaining(vertex -> {
		    Vertex newVertex = newGraph.addVertex(T.id, vertex.id(), T.label, vertex.label());
		    vertex.properties().forEachRemaining(vertexProperty -> {
			VertexProperty<?> newVertexProperty = newVertex.property(VertexProperty.Cardinality.list,
				vertexProperty.key(), vertexProperty.value(), T.id, vertexProperty.id());
			vertexProperty.properties().forEachRemaining(property -> {
			    newVertexProperty.property(property.key(), property.value());
			});
		    });
		});
		graph.edges().forEachRemaining(edge -> {
		    Vertex outVertex = newGraph.vertices(edge.outVertex().id()).next();
		    Vertex inVertex = newGraph.vertices(edge.inVertex().id()).next();
		    Edge newEdge = outVertex.addEdge(edge.label(), inVertex, T.id, edge.id());
		    edge.properties().forEachRemaining(property -> newEdge.property(property.key(), property.value()));
		});
		return newGraph;
	    }
	}
    }

    private void addPropertiesToOriginalGraph() {
	graph.dropGraphComputerView();
	computeProperties.forEach((element, properties) -> {
	    properties.forEach((key, vertexProperties) -> {
		vertexProperties.forEach(vertexProperty -> {
		    VertexProperty<?> newVertexProperty = ((Vertex) element).property(VertexProperty.Cardinality.list,
			    vertexProperty.key(), vertexProperty.value(), T.id, vertexProperty.id());
		    vertexProperty.properties().forEachRemaining(property -> {
			newVertexProperty.property(property.key(), property.value());
		    });
		});
	    });
	});
	computeProperties.clear();
    }

    private boolean isComputeKey(String key) {
	return computeKeys.contains(key);
    }

    private void addValue(Vertex vertex, String key, DuctileVertexProperty<?> property) {
	Map<String, List<VertexProperty<?>>> elementProperties = computeProperties.computeIfAbsent(vertex,
		k -> new ConcurrentHashMap<>());
	elementProperties.compute(key, (k, v) -> {
	    if (null == v)
		v = Collections.synchronizedList(new ArrayList<>());
	    v.add(property);
	    return v;
	});
    }

    private void removeValue(Vertex vertex, String key, DuctileVertexProperty<?> property) {
	computeProperties.computeIfPresent(vertex, (k, v) -> {
	    v.computeIfPresent(key, (k1, v1) -> {
		v1.remove(property);
		return v1;
	    });
	    return v;
	});
    }

    private List<VertexProperty<?>> getValue(Vertex vertex, String key) {
	return computeProperties.getOrDefault(vertex, Collections.emptyMap()).getOrDefault(key,
		Collections.emptyList());
    }

}
