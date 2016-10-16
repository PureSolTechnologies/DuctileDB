package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary;

import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngine;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyScanner;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class IndexedColumnFamilyScannerImpl implements ColumnFamilyScanner {

    private final ColumnFamilyEngine columnFamilyEngine;
    private final SecondaryIndexEngine indexEngine;
    private final byte[] fromValue;
    private final byte[] toValue;
    private final ColumnFamilyScanner scanner;

    public IndexedColumnFamilyScannerImpl(ColumnFamilyEngine columnFamilyEngine, SecondaryIndexEngine indexEngine,
	    byte[] fromValue, byte[] toValue) {
	this.columnFamilyEngine = columnFamilyEngine;
	this.indexEngine = indexEngine;
	this.fromValue = fromValue;
	this.toValue = toValue;
	this.scanner = indexEngine.getScanner(fromValue, toValue);
    }

    @Override
    public ColumnFamilyRow peek() {
	Key key = scanner.peek().getRowKey();
	return new ColumnFamilyRow(key, columnFamilyEngine.get(key.getKey()));
    }

    @Override
    public boolean hasNext() {
	return scanner.hasNext();
    }

    @Override
    public ColumnFamilyRow next() {
	Key key = scanner.next().getRowKey();
	Key cfKey = extractCFKey(key);
	if (indexEngine.getDescription().getIndexType() == IndexType.HEAP) {
	    return new ColumnFamilyRow(cfKey, columnFamilyEngine.get(cfKey.getKey()));
	} else {
	    ColumnMap columnMap = indexEngine.get(key.getKey());
	    return new ColumnFamilyRow(cfKey, columnMap);
	}
    }

    private Key extractCFKey(Key key) {
	SecondaryIndexDescriptor descriptor = indexEngine.getDescription();
	byte[] bytes = key.getKey();
	int pos = 0;
	for (int i = 0; i < descriptor.getColumns().size(); ++i) {
	    int len = Bytes.toInt(bytes, pos);
	    pos += 4 + len;
	}
	int len = Bytes.toInt(bytes, pos);
	pos += 4;
	byte[] cfKey = new byte[len];
	System.arraycopy(bytes, pos, cfKey, 0, len);
	return new Key(cfKey);
    }

    @Override
    public void close() throws IOException {
	scanner.close();
    }

}
