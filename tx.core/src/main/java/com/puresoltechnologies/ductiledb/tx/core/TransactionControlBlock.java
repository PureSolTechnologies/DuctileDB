package com.puresoltechnologies.ductiledb.tx.core;

import java.util.LinkedList;

public class TransactionControlBlock {

    private final LinkedList<LockControlBlock> lockControlBlocks = new LinkedList<>();

}
