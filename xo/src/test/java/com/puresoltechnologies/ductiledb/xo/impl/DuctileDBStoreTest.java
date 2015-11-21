package com.puresoltechnologies.ductiledb.xo.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.net.MalformedURLException;
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
    public void testNullHBaseSitePath() {
	assertNotNull(new DuctileDBStore(null, "keyspace"));
    }

    @Test
    public void testEmptyNamespace() throws MalformedURLException {
	try (DuctileDBStore store = new DuctileDBStore(new File("/opt/hbase/conf/hbase-site.xml"), "")) {
	    assertThat(store.getKeyspace(), is(DuctileDBStore.DEFAULT_DUCTILEDB_NAMESPACE));
	}
    }
}
