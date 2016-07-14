package com.puresoltechnologies.ductiledb.stores.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This is a DuctileDB storage for Hadoop.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class HadoopStorage implements Storage {

    private final FileSystem fileSystem;

    public HadoopStorage(Map<String, String> configuration) {
	super();
	this.fileSystem = null;
    }

    public void createFile() {
	// TODO
    }

    @Override
    public void initialize() throws IOException {
	// TODO Auto-generated method stub

    }

    @Override
    public void createDirectory(String storageName) throws IOException {
	// TODO Auto-generated method stub

    }

    @Override
    public void removeDirectory(String directory, boolean recursive) throws FileNotFoundException, IOException {
	// TODO Auto-generated method stub

    }

}
