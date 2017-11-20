package com.puresoltechnologies.ductiledb.columnfamily.index;

import java.io.IOException;

import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamily;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnMap;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.RowScanner;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

public class IndexedColumnFamilyScannerImpl implements ColumnFamilyScanner {

    private final ColumnFamily columnFamilyEngine;
    private final SecondaryIndexEngine indexEngine;
    private final ColumnValue fromValue;
    private final ColumnValue toValue;
    private final RowScanner scanner;

    public IndexedColumnFamilyScannerImpl(ColumnFamily columnFamilyEngine, SecondaryIndexEngine indexEngine,
	    ColumnValue fromValue, ColumnValue toValue) {
	this.columnFamilyEngine = columnFamilyEngine;
	this.indexEngine = indexEngine;
	this.fromValue = fromValue;
	this.toValue = toValue;
	this.scanner = indexEngine.getScanner(Key.of(fromValue.getBytes()), Key.of(toValue.getBytes()));
    }

    @Override
    public ColumnFamilyRow peek() {
	Key key = scanner.peek().getKey();
	return new ColumnFamilyRow(key, columnFamilyEngine.get(key));
    }

    @Override
    public boolean hasNext() {
	return scanner.hasNext();
    }

    @Override
    public ColumnFamilyRow next() {
	Key key = scanner.next().getKey();
	Key cfKey = extractCFKey(key);
	if (indexEngine.getDescription().getIndexType() == IndexType.HEAP) {
	    return new ColumnFamilyRow(cfKey, columnFamilyEngine.get(cfKey));
	} else {
	    try {
		return new ColumnFamilyRow(cfKey, ColumnMap.fromBytes(indexEngine.get(key)));
	    } catch (IOException e) {
		throw new StorageException("Could not read data.", e);
	    }
	}
    }

    private Key extractCFKey(Key key) {
	SecondaryIndexDescriptor descriptor = indexEngine.getDescription();
	byte[] bytes = key.getBytes();
	int pos = 0;
	for (int i = 0; i < descriptor.getColumns().size(); ++i) {
	    int len = Bytes.toInt(bytes, pos);
	    pos += 4 + len;
	}
	int len = Bytes.toInt(bytes, pos);
	pos += 4;
	byte[] cfKey = new byte[len];
	System.arraycopy(bytes, pos, cfKey, 0, len);
	return Key.of(cfKey);
    }

    @Override
    public void close() throws IOException {
	scanner.close();
    }

}
