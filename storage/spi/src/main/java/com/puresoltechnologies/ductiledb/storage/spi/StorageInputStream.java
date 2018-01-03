package com.puresoltechnologies.ductiledb.storage.spi;

import java.io.BufferedInputStream;
import java.io.InputStream;

import com.puresoltechnologies.streaming.streams.PositionInputStream;

public class StorageInputStream extends PositionInputStream implements StorageStream {

    public StorageInputStream(InputStream inputStream, int blockSize) {
	super(new BufferedInputStream(inputStream, blockSize));
    }

}
