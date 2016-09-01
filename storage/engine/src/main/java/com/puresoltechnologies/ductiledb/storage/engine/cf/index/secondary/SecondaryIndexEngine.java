package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary;

import com.puresoltechnologies.ductiledb.storage.engine.lss.LogStructuredStore;

/**
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public interface SecondaryIndexEngine extends LogStructuredStore {

    public SecondaryIndexDescriptor getDescription();

}
