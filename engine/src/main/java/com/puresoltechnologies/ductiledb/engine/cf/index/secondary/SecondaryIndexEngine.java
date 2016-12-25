package com.puresoltechnologies.ductiledb.engine.cf.index.secondary;

import com.puresoltechnologies.ductiledb.logstore.LogStructuredStore;

/**
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public interface SecondaryIndexEngine extends LogStructuredStore {

    public SecondaryIndexDescriptor getDescription();

}
