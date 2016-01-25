package com.puresoltechnologies.ductiledb.core.tx;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBAttachedVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class AttachedVertexIterable implements Iterable<DuctileDBVertex> {

    private final DuctileDBGraphImpl graph;
    private final DuctileDBTransactionImpl transaction;
    private final Iterator<Result> resultIterator;
    private final Iterator<DuctileDBCacheVertex> addedIterator;

    public AttachedVertexIterable(DuctileDBGraphImpl graph, DuctileDBTransactionImpl transaction,
	    ResultScanner resultScanner) {
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
	    public DuctileDBAttachedVertex next() {
		if (next != null) {
		    DuctileDBVertex result = next;
		    next = null;
		    return ElementUtils.toAttached(graph, result);
		}
		findNext();
		return ElementUtils.toAttached(graph, next);
	    }

	    private void findNext() {
		while ((next == null) && (resultIterator.hasNext())) {
		    Result result = resultIterator.next();
		    DuctileDBVertex vertex = ResultDecoder.toVertex(graph, transaction,
			    IdEncoder.decodeRowId(result.getRow()), result);
		    if (!transaction.wasVertexRemoved(vertex.getId())) {
			next = new DuctileDBAttachedVertex(graph, transaction, vertex.getId());
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
