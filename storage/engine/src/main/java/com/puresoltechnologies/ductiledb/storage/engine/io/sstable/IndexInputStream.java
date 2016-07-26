package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.BufferedInputStream;

import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBInputStream;

public class IndexInputStream extends DuctileDBInputStream {

    public IndexInputStream(BufferedInputStream bufferedOutputStream) {
	super(bufferedOutputStream);
    }

}
