package com.puresoltechnologies.ductiledb.storage.engine.utils;

import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]> {

    @Override
    public int compare(byte[] o1, byte[] o2) {
	if (o1.length == o2.length) {
	    for (int i = 0; i < o1.length; i++) {
		if (o1[i] != o2[i]) {
		    return o1[i] < o2[i] ? -i : +i;
		}
	    }
	    return 0;
	} else if (o1.length < o2.length) {
	    return -o1.length;
	} else {
	    return +o2.length;
	}
    }

}
