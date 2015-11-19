package com.puresoltechnologies.xo.titan.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.Test;

import com.buschmais.xo.api.XOException;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileDBStore;

public class DuctileDBStoreTest {

    @Test
    public void testRetrieveNamespace() throws URISyntaxException {
	String keyspace = DuctileDBStore
		.retrieveNamespaceFromURI(new URI("ductiledb:file:///opt/hbase/conf/hbase-site.xml"));
	assertEquals("keyspace", keyspace);

	keyspace = DuctileDBStore
		.retrieveNamespaceFromURI(new URI("ductiledb:file:///opt/hbase/conf/hbase-site.xml//"));
	assertEquals("keyspace", keyspace);
    }

    @Test
    public void testRetrieveEmptyNamespace() throws URISyntaxException {
	String keyspace = DuctileDBStore.retrieveNamespaceFromURI(new URI("protocol://host:1234"));
	assertEquals("", keyspace);

	keyspace = DuctileDBStore.retrieveNamespaceFromURI(new URI("protocol://host:1234//"));
	assertEquals("", keyspace);
    }

    @Test(expected = XOException.class)
    public void testRetrieveMultipleKeyspace() throws URISyntaxException {
	DuctileDBStore.retrieveNamespaceFromURI(new URI("protocol://host:1234/multi/path/"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullHBaseSitePath() {
	assertNotNull(new DuctileDBStore(null, "keyspace"));
    }

    @Test
    public void testEmptyNamespace() throws MalformedURLException {
	try (DuctileDBStore store = new DuctileDBStore(new URL("file:///opt/hbase/conf/hbase-site.xml"), "")) {
	    assertThat(store.getKeyspace(), is(DuctileDBStore.DEFAULT_DUCTILEDB_NAMESPACE));
	}
    }
}
