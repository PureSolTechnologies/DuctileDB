package com.puresoltechnologies.ductiledb.tx.core;

import java.util.Hashtable;

public class LockManager {

    private final Hashtable<ResourceIdentifier, ResourceControlBlock> resourceControlBlocks = new Hashtable<>();

}
