package com.puresoltechnologies.ductiledb;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.puresoltechnologies.ductiledb.utils.IdEncoder;
import com.tinkerpop.blueprints.Vertex;

public class VertexIterable implements Iterable<Vertex> {

    private final DuctileDBGraphImpl graph;
    private final Iterator<Result> resultIterator;

    public VertexIterable(DuctileDBGraphImpl graph, ResultScanner resultScanner) {
	super();
	this.graph = graph;
	resultIterator = resultScanner.iterator();
    }

    @Override
    public Iterator<Vertex> iterator() {
	return new Iterator<Vertex>() {

	    @Override
	    public boolean hasNext() {
		return resultIterator.hasNext();
	    }

	    @Override
	    public Vertex next() {
		Result result = resultIterator.next();
		return ResultDecoder.toVertex(graph, IdEncoder.decodeRowId(result.getRow()), result);
	    }
	};
    }

}
