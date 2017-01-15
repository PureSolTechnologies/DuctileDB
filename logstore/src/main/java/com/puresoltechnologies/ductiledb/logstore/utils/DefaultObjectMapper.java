package com.puresoltechnologies.ductiledb.logstore.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.puresoltechnologies.ductiledb.logstore.LogStructuredStore;

/**
 * This is the central place to get the {@link ObjectMapper} for
 * {@link LogStructuredStore} and all other dependent classes with standard
 * settings.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class DefaultObjectMapper {

    private static final ObjectMapper objectMapper;
    static {
	objectMapper = new ObjectMapper();
	objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    /**
     * Returns the {@link ObjectMapper} instance to be used.
     */
    public static ObjectMapper getInstance() {
	return objectMapper;
    }

}
