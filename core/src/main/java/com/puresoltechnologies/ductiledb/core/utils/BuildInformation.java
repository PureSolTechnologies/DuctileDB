package com.puresoltechnologies.ductiledb.core.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class BuildInformation {

    private static final Properties buildProperties = new Properties();

    static {
	try (InputStream stream = BuildInformation.class.getResourceAsStream("/META-INF/ductiledb_build.properties")) {
	    buildProperties.load(stream);
	} catch (IOException e) {
	    throw new RuntimeException("Could not read build.properties.", e);
	}
    }

    public static String getVersion() {
	return buildProperties.get("project.version").toString();
    }

    public static String getInceptionYear() {
	return buildProperties.get("project.inceptionYear").toString();
    }

    public static String getBuildYear() {
	return buildProperties.get("project.buildYear").toString();
    }

}
