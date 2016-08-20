package com.puresoltechnologies.ductiledb.xo.impl;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

public class DuctileDBStoreTest {

    @Test
    public void testURI() throws URISyntaxException {
	URI uri = new URI("ductiledb:/opt/hbase/conf/hbase-site.xml?schema=ductiledb;schema2=ductiledb2;");
	System.out.println("schema: " + uri.getScheme());
	System.out.println("host: " + uri.getHost());
	System.out.println("port: " + uri.getPort());
	System.out.println("path: " + uri.getPath());
	System.out.println("authority: " + uri.getAuthority());
	System.out.println("fragement: " + uri.getFragment());
	System.out.println("query: " + uri.getQuery());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullConfigFilePath() throws IOException {
	assertNotNull(new DuctileStore(null));
    }

}
