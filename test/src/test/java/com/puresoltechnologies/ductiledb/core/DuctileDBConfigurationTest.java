package com.puresoltechnologies.ductiledb.core;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

public class DuctileDBConfigurationTest {

    @Test
    public void test() throws IOException {
	Yaml yaml = new Yaml();
	try (InputStream inputStream = getClass().getResourceAsStream("/ductiledb-test.yml")) {
	    yaml.loadAs(inputStream, DuctileDBConfiguration.class);
	}
    }

}
