package com.puresoltechnologies.ductiledb.xo.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.buschmais.xo.api.ResultIterator;
import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.datastore.DatastoreQuery;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;
import com.puresoltechnologies.ductiledb.tinkerpop.gremlin.GremlinQueryExecutor;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Query;
import com.puresoltechnologies.ductiledb.xo.api.annotation.QueryLanguage;
import com.puresoltechnologies.ductiledb.xo.impl.query.gremlin.GremlinExpression;

public class DuctileDatastoreQuery implements DatastoreQuery<Query> {

    private final DuctileGraph ductileGraph;

    DuctileDatastoreQuery(DuctileGraph ductileGraph) {
	this.ductileGraph = ductileGraph;
    }

    @Override
    public ResultIterator<Map<String, Object>> execute(String query, Map<String, Object> parameters) {
	if (query.startsWith("gremlin:")) {
	    GremlinExpression gremlinExpression = GremlinExpression.createGremlinExpression(query.substring(8),
		    parameters);
	    return executeGremlin(gremlinExpression);
	} else {
	    GremlinExpression gremlinExpression = GremlinExpression.createGremlinExpression(query, parameters);
	    return executeGremlin(gremlinExpression);
	}
    }

    @Override
    public ResultIterator<Map<String, Object>> execute(Query query, Map<String, Object> parameters) {
	if (query.language() == QueryLanguage.GREMLIN) {
	    GremlinExpression gremlinExpression = GremlinExpression.createGremlinExpression(query, parameters);
	    return executeGremlin(gremlinExpression);
	} else {
	    throw new XOException("Query language '" + query.language().name() + "' is not supported.");
	}
    }

    private ResultIterator<Map<String, Object>> executeGremlin(GremlinExpression gremlinExpression) {
	GremlinQueryExecutor gremlinQueryExecutor = ductileGraph.createGremlinQueryExecutor();
	List<Object> results = gremlinQueryExecutor.query(gremlinExpression.getExpression());
	Iterator<Object> resultIterator = results.iterator();
	return new ResultIterator<Map<String, Object>>() {

	    @Override
	    public boolean hasNext() {
		return resultIterator.hasNext();
	    }

	    @Override
	    public Map<String, Object> next() {
		Map<String, Object> results = new HashMap<>();
		Object next = resultIterator.next();
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
		resultIterator.remove();
	    }

	    @Override
	    public void close() {
		// there is no close required in pipe
	    }
	};

    }
}
