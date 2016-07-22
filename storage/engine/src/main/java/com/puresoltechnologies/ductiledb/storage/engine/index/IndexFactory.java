package com.puresoltechnologies.ductiledb.storage.engine.index;

import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
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
     * @param columnFamilyDescriptor
     *            is used to get all information for the column family to get
     *            indexed.
     * 
     * @return An {@link Index} is returned to be used by column families to
     *         find entries in database files.
     */
    public static Index create(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor) {
	return new IndexImpl(storage, columnFamilyDescriptor);
    }

}
