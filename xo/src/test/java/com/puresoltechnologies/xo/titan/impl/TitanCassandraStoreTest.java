package com.puresoltechnologies.xo.titan.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

import com.buschmais.xo.api.XOException;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileDBStore;

public class TitanCassandraStoreTest {

	@Test
	public void testRetrieveKeyspace() throws URISyntaxException {
		String keyspace = DuctileDBStore.retrieveKeyspaceFromURI(new URI(
				"protocol://host:1234/keyspace"));
		assertEquals("keyspace", keyspace);

		keyspace = DuctileDBStore.retrieveKeyspaceFromURI(new URI(
				"protocol://host:1234/keyspace//"));
		assertEquals("keyspace", keyspace);
	}

	@Test
	public void testRetrieveEmptyKeyspace() throws URISyntaxException {
		String keyspace = DuctileDBStore.retrieveKeyspaceFromURI(new URI(
				"protocol://host:1234"));
		assertEquals("", keyspace);

		keyspace = DuctileDBStore.retrieveKeyspaceFromURI(new URI(
				"protocol://host:1234//"));
		assertEquals("", keyspace);
	}

	@Test(expected = XOException.class)
	public void testRetrieveMultipleKeyspace() throws URISyntaxException {
		DuctileDBStore.retrieveKeyspaceFromURI(new URI(
				"protocol://host:1234/multi/path/"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullHost() {
		new DuctileDBStore(null, 123, "keyspace");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmptyHost() {
		new DuctileDBStore("", 123, "keyspace");
	}

	@Test
	public void testNegativePort() {
		DuctileDBStore store = new DuctileDBStore("host", -1,
				"keyspace");
		assertThat(store.getPort(),
				is(DuctileDBStore.DEFAULT_CASSANDRA_THRIFT_PORT));
	}

	@Test
	public void testZeroPort() {
		DuctileDBStore store = new DuctileDBStore("host", 0,
				"keyspace");
		assertThat(store.getPort(),
				is(DuctileDBStore.DEFAULT_CASSANDRA_THRIFT_PORT));
	}

	@Test
	public void testNullKeyspace() {
		DuctileDBStore store = new DuctileDBStore("host", 123, null);
		assertThat(store.getKeyspace(),
				is(DuctileDBStore.DEFAULT_TITAN_KEYSPACE));
	}

	@Test
	public void testEmptyKeyspace() {
		DuctileDBStore store = new DuctileDBStore("host", 123, "");
		assertThat(store.getKeyspace(),
				is(DuctileDBStore.DEFAULT_TITAN_KEYSPACE));
	}
}
