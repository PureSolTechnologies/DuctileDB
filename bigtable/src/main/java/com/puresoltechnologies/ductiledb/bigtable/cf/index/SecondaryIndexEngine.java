package com.puresoltechnologies.ductiledb.bigtable.cf.index;

import com.puresoltechnologies.ductiledb.logstore.LogStructuredStore;

/**
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public interface SecondaryIndexEngine extends LogStructuredStore {

    public SecondaryIndexDescriptor getDescription();

}
