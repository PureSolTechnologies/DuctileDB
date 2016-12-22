package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.util.Iterator;

import com.puresoltechnologies.ductiledb.core.graph.DuctileDBAttachedEdge;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.core.graph.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.engine.Result;
import com.puresoltechnologies.ductiledb.engine.ResultScanner;

public class AttachedEdgeIterable implements Iterable<DuctileDBEdge> {

    private final DuctileDBTransactionImpl transaction;
    private final Iterator<Result> resultIterator;
    private final Iterator<DuctileDBCacheEdge> addedIterator;

    public AttachedEdgeIterable(DuctileDBTransactionImpl transaction, ResultScanner resultScanner) {
	super();
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
	    public DuctileDBAttachedEdge next() {
		if (next != null) {
		    DuctileDBEdge result = next;
		    next = null;
		    return ElementUtils.toAttached(result);
		}
		findNext();
		return ElementUtils.toAttached(next);
	    }

	    private void findNext() {
		while ((next == null) && (resultIterator.hasNext())) {
		    Result result = resultIterator.next();
		    DuctileDBEdge edge = ResultDecoder.toCacheEdge(transaction,
			    IdEncoder.decodeRowId(result.getRowKey().getBytes()), result);
		    if (!transaction.wasEdgeRemoved(edge.getId())) {
			next = new DuctileDBAttachedEdge(transaction, edge.getId());
		    }
		}
		while ((next == null) && (addedIterator.hasNext())) {
		    DuctileDBEdge edge = addedIterator.next();
		    if (!transaction.wasEdgeRemoved(edge.getId())) {
			next = edge;
		    }
		}
	    }
	};
    }

}
