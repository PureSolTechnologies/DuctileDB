package com.puresoltechnologies.ductiledb.columnfamily.fullindex;

import java.io.Closeable;

import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.logstore.Key;

public interface FullIndexEngine extends Closeable {

    public ColumnFamilyScanner find(Key columnKey, ColumnValue value);

    public ColumnFamilyScanner find(Key columnKey, ColumnValue fromValue, ColumnValue toValue);

}
