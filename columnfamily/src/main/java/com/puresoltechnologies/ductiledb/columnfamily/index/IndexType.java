package com.puresoltechnologies.ductiledb.columnfamily.index;

/**
 * This enum is used to identify the type of the secondary index.
 * 
 * @author Rick-Rainer Ludwig
 */
public enum IndexType {

    /**
     * This index type only stores the key to the entry containing the desired
     * data. The data is to be read from the original storage location.
     * 
     * This index type is efficient for writes, but needs an additional hop from
     * index to data file for reading.
     */
    HEAP,
    /**
     * This index types stores the whole information to the index, so reading of
     * the data can be done directly from the index table.
     * 
     * This index type is expensive for writing, because the whole data is to be
     * stored multiple times. Reading is efficient for this type of index.
     */
    CLUSTERED;

}
