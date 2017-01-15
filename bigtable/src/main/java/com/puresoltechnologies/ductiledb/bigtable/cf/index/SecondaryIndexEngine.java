package com.puresoltechnologies.ductiledb.bigtable.cf.index;

import com.puresoltechnologies.ductiledb.logstore.StorageOperations;

/**
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public interface SecondaryIndexEngine extends StorageOperations {

    public SecondaryIndexDescriptor getDescription();

}
