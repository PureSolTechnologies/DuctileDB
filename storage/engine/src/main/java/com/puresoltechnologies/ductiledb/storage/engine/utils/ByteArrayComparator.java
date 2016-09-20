package com.puresoltechnologies.ductiledb.storage.engine.utils;

import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]> {

    private static final ByteArrayComparator instance = new ByteArrayComparator();

    public static ByteArrayComparator getInstance() {
	return instance;
    }

    private ByteArrayComparator() {
    }

    @Override
    public int compare(byte[] o1, byte[] o2) {
	for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
	    if (o1[i] != o2[i]) {
		return (o1[i] & 0xFF) - (o2[i] & 0xFF);
	    }
	}
	if (o1.length == o2.length) {
	    return 0;
	} else if (o1.length < o2.length) {
	    return -1;
	} else {
	    return +1;
	}
    }

}
