package com.puresoltechnologies.ductiledb.logstore;

public interface StorageOperations extends AutoCloseable {

    public void open();

    @Override
    public void close();

    /**
     * This method is used to put additional columns to the given row.
     * 
     * @param rowKey
     * @param columnValues
     */
    public void put(Key rowKey, byte[] columnValues);

    /**
     * This metho retrieves the columns from the given row.
     * 
     * @param rowKey
     * @return
     */
    public byte[] get(Key rowKey);

    /**
     * This method returns a scanner for the column family.
     * 
     * @return
     */
    public RowScanner getScanner(Key startRowKey, Key endRowKey);

    /**
     * This method removes the given row.
     * 
     * @param rowKey
     */
    public void delete(Key rowKey);

}
