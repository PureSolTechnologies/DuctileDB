package com.puresoltechnologies.ductiledb.columnfamily;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.utils.DefaultObjectMapper;

public class ColumnFamilyDescriptorTest {

    @Test
    public void testSerialization() throws IOException {
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(Key.of("ColumnFamilyName"),
		new File("/Directory"));

	String jsonString = objectMapper.writeValueAsString(descriptor);
	ColumnFamilyDescriptor descriptor2 = objectMapper.readValue(jsonString, ColumnFamilyDescriptor.class);
	assertEquals(descriptor, descriptor2);
    }

}
