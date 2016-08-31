package com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class IndexImplTest {

    @Test
    public void test() {
	IndexImpl index = new IndexImpl(mock(Storage.class), mock(File.class));

	index.put(new IndexEntry(new Key(Bytes.toBytes(0l)), new File("File0"), 0l));
	index.put(new IndexEntry(new Key(Bytes.toBytes(1l)), new File("File1"), 1l));
	long a = 0;
	long b = 1;
	for (int i = 2; i <= 25; ++i) {
	    long f = a + b;
	    byte[] rowKey = Bytes.toBytes(f);
	    index.put(new IndexEntry(new Key(rowKey), new File("File" + i), f));
	    a = b;
	    b = f;
	    System.out.println(f);
	}
	for (int i = 0; i <= b; ++i) {
	    OffsetRange range = index.find(new Key(Bytes.toBytes((long) i)));
	    IndexEntry startOffset = range.getStartOffset();
	    IndexEntry endOffset = range.getEndOffset();
	    // System.out.println("------");
	    // System.out.println(Bytes.toHumanReadableString(Bytes.toBytes((long)
	    // i)));
	    // System.out.println(startOffset);
	    // System.out.println(endOffset);
	    assertTrue(startOffset.getRowKey().compareTo(endOffset.getRowKey()) <= 0);
	}
    }

}
