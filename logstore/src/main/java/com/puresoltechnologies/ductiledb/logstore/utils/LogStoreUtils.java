package com.puresoltechnologies.ductiledb.logstore.utils;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.puresoltechnologies.ductiledb.logstore.LogStructuredStore;

/**
 * This class contains utilities which are used to handle column families in
 * different places like the engine, index and compactor.
 * 
 * @author Rick-Rainer Ludwig
 */
public class LogStoreUtils {

    private static final Pattern METADATA_FILE_PATTERN = Pattern
	    .compile(LogStructuredStore.DB_FILE_PREFIX + "-(\\d+)" + LogStructuredStore.METADATA_SUFFIX);

    /**
     * The method returns the timestamp out of a metadata file name.
     * 
     * @param metadataFileName
     *            is the file name of the meta data.
     * @return A {@link String} is returned containing the timestamp.
     * @throws IOException
     */
    public static String extractTimestampForMetadataFile(String metadataFileName) throws IOException {
	Matcher matcher = METADATA_FILE_PATTERN.matcher(metadataFileName);
	if (matcher.matches()) {
	    return matcher.group(1);
	} else {
	    throw new IOException("The provided file name '' is not a meta data file.");
	}
    }

}
