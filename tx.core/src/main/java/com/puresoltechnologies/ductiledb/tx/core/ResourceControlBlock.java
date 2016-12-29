package com.puresoltechnologies.ductiledb.tx.core;

/**
 * This class keeps information about a resource locked or to be locked.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class ResourceControlBlock {

    private final ResourceIdentifier resourceIdentifier;
    private final LockControlBlockQueue lockControlBlocks = new LockControlBlockQueue();

    public ResourceControlBlock(ResourceIdentifier resourceIdentifier) {
	super();
	this.resourceIdentifier = resourceIdentifier;
    }

}
