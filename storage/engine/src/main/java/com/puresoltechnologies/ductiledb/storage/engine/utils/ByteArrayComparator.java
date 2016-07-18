package com.puresoltechnologies.ductiledb.storage.engine.utils;

import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]> {

    @Override
    public int compare(byte[] o1, byte[] o2) {
	int minLength = Math.min(o1.length, o2.length);
	for (int i = 0; i < minLength; i++) {
	    if (o1[i] < o2[i]) {
		return -i;
	    }
	    if (o1[i] > o2[i]) {
		return +i;
	    }
	}
	if (o1.length < o2.length) {
	    return -minLength;
	}
	if (o1.length > o2.length) {
	    return +minLength;
	}
	return 0;
    }

}
