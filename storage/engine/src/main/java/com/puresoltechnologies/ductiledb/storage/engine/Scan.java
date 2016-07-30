package com.puresoltechnologies.ductiledb.storage.engine;

public class Scan {

    private final byte[] startRow;
    private final byte[] endRow;

    public Scan() {
	this(null, null);
    }

    public Scan(byte[] startRow) {
	this(startRow, null);
    }

    public Scan(byte[] startRow, byte[] endRow) {
	super();
	this.startRow = startRow;
	this.endRow = endRow;
    }

    public byte[] getStartRow() {
	return startRow;
    }

    public byte[] getEndRow() {
	return endRow;
    }

}
