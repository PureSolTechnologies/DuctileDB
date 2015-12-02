package com.puresoltechnologies.ductiledb.core;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class EdgeIterable implements Iterable<DuctileDBEdge> {

    private final DuctileDBGraphImpl graph;
    private final Iterator<Result> resultIterator;

    public EdgeIterable(DuctileDBGraphImpl graph, ResultScanner resultScanner) {
	super();
	this.graph = graph;
	resultIterator = resultScanner.iterator();
    }

    @Override
    public Iterator<DuctileDBEdge> iterator() {
	return new Iterator<DuctileDBEdge>() {

	    @Override
	    public boolean hasNext() {
		return resultIterator.hasNext();
	    }

	    @Override
	    public DuctileDBEdge next() {
		Result result = resultIterator.next();
		return ResultDecoder.toEdge(graph, IdEncoder.decodeRowId(result.getRow()), result);
	    }
	};
    }

}
