package com.puresoltechnologies.ductiledb.engine.cf;

import java.util.Set;

import com.puresoltechnologies.ductiledb.engine.cf.index.secondary.SecondaryIndexDescriptor;
import com.puresoltechnologies.ductiledb.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.RowScanner;

/**
 * This class handles the storage of a single column family.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public interface ColumnFamilyEngine extends AutoCloseable {

    public void open();

    public Key getName();

    public ColumnFamilyDescriptor getDescriptor();

    /**
     * This method is used to put additional columns to the given row.
     * 
     * @param rowKey
     * @param columnValues
     */
    public void put(Key rowKey, ColumnMap columnMap);

    /**
     * This method retrieves the columns from the given row.
     * 
     * @param rowKey
     * @return
     */
    public ColumnMap get(Key rowKey);

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

    public void delete(Key rowKey, Set<Key> columns);

    /**
     * This method searches the store for a specified column/value pair.
     * 
     * @param columnKey
     *            is the column key of the column to be checked for a certain
     *            value.
     * @param value
     *            is the value to be found in the specified column.
     * @return A {@link RowScanner} is returned to scan over all found results.
     *         If there is not index for the column, <code>null</code> is
     *         returned.
     */
    public RowScanner find(Key columnKey, ColumnValue value);

    /**
     * This method searches the store for a specified column/value pair.
     * 
     * 
     * @param columnKey
     *            is the column key of the column to be checked for a certain
     *            value.
     * @param fromValue
     *            is the minimum value to be found in the specified column.
     * @param toValue
     *            is the maximum value to be found in the specified column.
     * @return A {@link RowScanner} is returned to scan over all found results.
     *         If there is not index for the column, <code>null</code> is
     *         returned.
     */
    public RowScanner find(Key columnKey, ColumnValue fromValue, ColumnValue toValue);

    /**
     * Increments (or decrements) a value in atomic way.
     * 
     * @param rowKey
     * @param column
     * @param incrementValue
     */
    public long incrementColumnValue(Key rowKey, Key column, long incrementValue);

    /**
     * Increments (or decrements) a value in atomic way.
     * 
     * @param rowKey
     * @param column
     * @param startValue
     * @param incrementValue
     */
    public long incrementColumnValue(Key rowKey, Key column, long startValue, long incrementValue);

    public void createIndex(SecondaryIndexDescriptor indexDescriptor);

    public void dropIndex(String name);

    public SecondaryIndexDescriptor getIndex(String name);

    public Iterable<SecondaryIndexDescriptor> getIndizes();
}
