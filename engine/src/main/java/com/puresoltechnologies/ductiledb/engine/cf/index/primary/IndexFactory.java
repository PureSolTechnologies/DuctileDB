package com.puresoltechnologies.ductiledb.engine.cf.index.primary;

import java.io.File;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is used to create the index.
 * 
 * @author Rick-Rainer Ludwig
 */
public class IndexFactory {

    /**
     * This method creates a new index.
     * 
     * @param storage
     *            is the {@link Storage} to be used to read and create the
     *            index.
     * @param directory
     *            is used to get all information for the column family to get
     *            indexed.
     * 
     * @return An {@link Index} is returned to be used by column families to
     *         find entries in database files.
     */
    public static Index create(Storage storage, File directory) {
	return new IndexImpl(storage, directory);
    }

    public static Index create(Storage storage, File directory, File metadataFile) {
	return new IndexImpl(storage, directory, metadataFile);
    }

}
