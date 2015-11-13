package com.puresoltechnologies.ductiledb.xo.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.buschmais.xo.api.ResultIterator;
import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.datastore.DatastoreQuery;
import com.puresoltechnologies.ductiledb.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Gauging;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.pipes.Pipe;

public class DuctileQuery implements DatastoreQuery<Gauging> {

    private final DuctileDBGraph graph;

    DuctileQuery(DuctileDBGraph graph) {
	this.graph = graph;
    }

    @Override
    public ResultIterator<Map<String, Object>> execute(String query, Map<String, Object> parameters) {
	final GaugingExpression gremlinExpression = GaugingManager.getGaugingExpression(query, parameters);
	return execute(parameters, gremlinExpression);
    }

    @Override
    public ResultIterator<Map<String, Object>> execute(Gauging query, Map<String, Object> parameters) {
	final GaugingExpression gremlinExpression = GaugingManager.getGaugingExpression(query, parameters);
	return execute(parameters, gremlinExpression);
    }

    private ResultIterator<Map<String, Object>> execute(Map<String, Object> parameters,
	    final GaugingExpression gremlinExpression) {
	String expression = gremlinExpression.getExpression();
	@SuppressWarnings("unchecked")
	final Pipe<Vertex, Vertex> pipe = DuctileQuery.compile(expression);
	if (parameters.containsKey("this")) {
	    Object setThis = parameters.get("this");
	    if (Vertex.class.isAssignableFrom(setThis.getClass())) {
		DuctileDBVertex vertex = (DuctileDBVertex) setThis;
		pipe.setStarts(Arrays.asList(vertex));
	    } else if (Edge.class.isAssignableFrom(setThis.getClass())) {
		DuctileDBEdge edge = (DuctileDBEdge) setThis;
		pipe.setStarts(Arrays.asList(edge.getVertex(Direction.IN), edge.getVertex(Direction.OUT)));
	    } else {
		throw new XOException(
			"Unsupported start point '" + String.valueOf(setThis) + "' (class=" + setThis.getClass() + ")");
	    }
	} else {
	    pipe.setStarts(graph.query().vertices());
	}
	return new ResultIterator<Map<String, Object>>() {

	    @Override
	    public boolean hasNext() {
		return pipe.hasNext();
	    }

	    @Override
	    public Map<String, Object> next() {
		Map<String, Object> results = new HashMap<>();
		Object next = pipe.next();
		if (next instanceof Vertex) {
		    results.put(gremlinExpression.getResultName(), next);
		} else if (next instanceof Edge) {
		    results.put(gremlinExpression.getResultName(), next);
		} else if (next instanceof Map) {
		    @SuppressWarnings("unchecked")
		    Map<String, Object> map = (Map<String, Object>) next;
		    results.putAll(map);
		} else {
		    results.put("unknown_type", next);
		}
		return results;
	    }

	    @Override
	    public void remove() {
		pipe.remove();
	    }

	    @Override
	    public void close() {
		// there is no close required in pipe
	    }
	};
    }

    private static Pipe<Vertex, Vertex> compile(String expression) {
	// TODO Auto-generated method stub
	return null;
    }
}
