package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.util.Iterator;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBAttachedVertex;
import com.puresoltechnologies.ductiledb.core.graph.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.storage.engine.Result;

public class AttachedVertexIterable implements Iterable<DuctileDBVertex> {

    private final DuctileDBTransactionImpl transaction;
    private final Iterator<Result> resultIterator;
    private final Iterator<DuctileDBCacheVertex> addedIterator;

    public AttachedVertexIterable(DuctileDBTransactionImpl transaction, ResultScanner resultScanner) {
	super();
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
		    return ElementUtils.toAttached(result);
		}
		findNext();
		return ElementUtils.toAttached(next);
	    }

	    private void findNext() {
		while ((next == null) && (resultIterator.hasNext())) {
		    Result result = resultIterator.next();
		    DuctileDBVertex vertex = ResultDecoder.toVertex(transaction, IdEncoder.decodeRowId(result.getRow()),
			    result);
		    if (!transaction.wasVertexRemoved(vertex.getId())) {
			next = new DuctileDBAttachedVertex(transaction, vertex.getId());
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
