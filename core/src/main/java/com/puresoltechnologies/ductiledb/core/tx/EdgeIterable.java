package com.puresoltechnologies.ductiledb.core.tx;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.core.DuctileDBEdgeImpl;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.ResultDecoder;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class EdgeIterable implements Iterable<DuctileDBEdge> {

    private final DuctileDBGraphImpl graph;
    private final DuctileDBTransactionImpl transaction;
    private final Iterator<Result> resultIterator;
    private final Iterator<DuctileDBEdge> addedIterator;

    public EdgeIterable(DuctileDBGraphImpl graph, DuctileDBTransactionImpl transaction, ResultScanner resultScanner) {
	super();
	this.graph = graph;
	this.transaction = transaction;
	resultIterator = resultScanner.iterator();
	addedIterator = transaction.addedEdges().iterator();
    }

    @Override
    public Iterator<DuctileDBEdge> iterator() {
	return new Iterator<DuctileDBEdge>() {

	    private DuctileDBEdge next = null;

	    @Override
	    public boolean hasNext() {
		if (next != null) {
		    return true;
		}
		findNext();
		return next != null;
	    }

	    @Override
	    public DuctileDBEdge next() {
		if (next != null) {
		    DuctileDBEdge result = next;
		    next = null;
		    return result;
		}
		findNext();
		return next;
	    }

	    private void findNext() {
		while ((next == null) && (resultIterator.hasNext())) {
		    Result result = resultIterator.next();
		    DuctileDBEdgeImpl edge = ResultDecoder.toEdge(graph, IdEncoder.decodeRowId(result.getRow()),
			    result);
		    if (!transaction.wasEdgeRemoved(edge.getId())) {
			DuctileDBEdge cachedEdge = transaction.getCachedEdge(edge.getId());
			if (cachedEdge != null) {
			    next = cachedEdge;

			} else {
			    next = edge;
			}
			next = edge;
		    }
		}
		while ((next == null) && (addedIterator.hasNext())) {
		    DuctileDBEdge vertex = addedIterator.next();
		    if (!transaction.wasEdgeRemoved(vertex.getId())) {
			next = vertex;
		    }
		}
	    }
	};
    }

}
