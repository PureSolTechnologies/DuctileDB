package com.puresoltechnologies.ductiledb.bigtable.cf;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.puresoltechnologies.ductiledb.bigtable.TableDescriptor;
import com.puresoltechnologies.ductiledb.logstore.utils.DefaultObjectMapper;

public class TableDescriptorTest {

    @Test
    public void testSerialization() throws IOException {
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	TableDescriptor descriptor = new TableDescriptor("TableName", "TableDescription", new File("/Directory"));

	String jsonString = objectMapper.writeValueAsString(descriptor);
	TableDescriptor descriptor2 = objectMapper.readValue(jsonString, TableDescriptor.class);
	assertEquals(descriptor, descriptor2);
    }

}
