package com.puresoltechnologies.ductiledb.storage.engine.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IOUtils {

    public static void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
	byte[] buffer = new byte[4096];
	int len;
	while ((len = inputStream.read(buffer)) >= 0) {
	    outputStream.write(buffer, 0, len);
	}
    }

}
