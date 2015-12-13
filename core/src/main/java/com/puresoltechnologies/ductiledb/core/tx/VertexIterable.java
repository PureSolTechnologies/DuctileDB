package com.puresoltechnologies.ductiledb.core.tx;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.ResultDecoder;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class VertexIterable implements Iterable<DuctileDBVertex> {

    private final DuctileDBGraphImpl graph;
    private final DuctileDBTransactionImpl transaction;
    private final Iterator<Result> resultIterator;
    private final Iterator<DuctileDBVertex> addedIterator;

    public VertexIterable(DuctileDBGraphImpl graph, DuctileDBTransactionImpl transaction, ResultScanner resultScanner) {
	super();
	this.graph = graph;
	this.transaction = transaction;
	resultIterator = resultScanner.iterator();
	addedIterator = transaction.addedVertices().iterator();
    }

    @Override
    public Iterator<DuctileDBVertex> iterator() {
	return new Iterator<DuctileDBVertex>() {

	    private DuctileDBVertex next = null;

	    @Override
	    public boolean hasNext() {
		if (next != null) {
		    return true;
		}
		findNext();
		return next != null;
	    }

	    @Override
	    public DuctileDBVertex next() {
		if (next != null) {
		    DuctileDBVertex result = next;
		    next = null;
		    return result;
		}
		findNext();
		return next;
	    }

	    private void findNext() {
		while ((next == null) && (resultIterator.hasNext())) {
		    Result result = resultIterator.next();
		    DuctileDBVertexImpl vertex = ResultDecoder.toVertex(graph, IdEncoder.decodeRowId(result.getRow()),
			    result);
		    if (!transaction.wasVertexRemoved(vertex.getId())) {
			DuctileDBVertex cachedVertex = transaction.getCachedVertex(vertex.getId());
			if (cachedVertex != null) {
			    next = cachedVertex;

			} else {
			    next = vertex;
			}
		    }
		}
		while ((next == null) && (addedIterator.hasNext())) {
		    DuctileDBVertex vertex = addedIterator.next();
		    if (!transaction.wasVertexRemoved(vertex.getId())) {
			next = vertex;
		    }
		}
	    }
	};
    }

}
