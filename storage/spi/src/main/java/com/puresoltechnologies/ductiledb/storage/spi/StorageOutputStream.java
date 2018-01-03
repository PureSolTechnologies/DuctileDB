package com.puresoltechnologies.ductiledb.storage.spi;

import java.io.BufferedOutputStream;
import java.io.OutputStream;

import com.puresoltechnologies.streaming.streams.PositionOutputStream;

public class StorageOutputStream extends PositionOutputStream implements StorageStream {

    public StorageOutputStream(OutputStream outputStream, int blockSize) {
	super(new BufferedOutputStream(outputStream, blockSize));
    }

}
