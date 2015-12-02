package com.puresoltechnologies.ductiledb.core;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.puresoltechnologies.ductiledb.api.Vertex;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

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
