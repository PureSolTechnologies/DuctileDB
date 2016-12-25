package com.puresoltechnologies.ductiledb.logstore.index;

/**
 * This class represents an offset range from index in which the actual key can
 * be found. In case the actual key was by chance in index, the start offset and
 * end offset are the same and representing the actual key.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class OffsetRange {

    private final IndexEntry startOffset;
    private final IndexEntry endOffset;

    public OffsetRange(IndexEntry startOffset, IndexEntry endOffset) {
	super();
	this.startOffset = startOffset;
	this.endOffset = endOffset;
    }

    public IndexEntry getStartOffset() {
	return startOffset;
    }

    public IndexEntry getEndOffset() {
	return endOffset;
    }

    @Override
    public String toString() {
	return startOffset.toString() + " -> " + endOffset.toString();
    }
}
