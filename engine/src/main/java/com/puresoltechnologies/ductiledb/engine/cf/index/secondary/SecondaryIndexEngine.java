package com.puresoltechnologies.ductiledb.engine.cf.index.secondary;

import com.puresoltechnologies.ductiledb.engine.lss.LogStructuredStore;

/**
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public interface SecondaryIndexEngine extends LogStructuredStore {

    public SecondaryIndexDescriptor getDescription();

}
