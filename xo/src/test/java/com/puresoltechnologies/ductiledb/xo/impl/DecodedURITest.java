package com.puresoltechnologies.ductiledb.xo.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.buschmais.xo.api.XOException;

public class DecodedURITest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testValidURI() throws URISyntaxException {
	URI uri = new URI("ductiledb:/opt/hbase/conf/hbase-site.xml?namespace=mynamespace");
	DecodedURI decodedURI = new DecodedURI(uri);
	assertSame(uri, decodedURI.getUri());
	assertEquals(new File("/opt/hbase/conf/hbase-site.xml"), decodedURI.getPath());
	assertEquals("mynamespace", decodedURI.getNamespace());
    }

    @Test
    public void testValidURI2() throws URISyntaxException {
	URI uri = new URI("ductiledb:/opt/hbase/conf/hbase-site.xml");
	DecodedURI decodedURI = new DecodedURI(uri);
	assertSame(uri, decodedURI.getUri());
	assertEquals(new File("/opt/hbase/conf/hbase-site.xml"), decodedURI.getPath());
	assertEquals(DecodedURI.DUCTILEDB_SCHEME, decodedURI.getNamespace());
    }

    @Test
    public void testInvalidURIWithWrongProtocol() throws URISyntaxException {
	expectedException.expect(XOException.class);
	expectedException.expectMessage("Scheme 'wrong' does not match expected scheme 'ductiledb'.");
	URI uri = new URI("wrong:/opt/hbase/conf/hbase-site.xml");
	new DecodedURI(uri);
    }

    @Test
    public void testInvalidURIWithProvidedHost() throws URISyntaxException {
	expectedException.expect(XOException.class);
	expectedException.expectMessage("A host name is not supported in DuctileDB URI.");
	URI uri = new URI("ductiledb://host/opt/hbase/conf/hbase-site.xml");
	new DecodedURI(uri);
    }

    @Test
    public void testInvalidURIWithUnsupportPropertyKey() throws URISyntaxException {
	expectedException.expect(XOException.class);
	expectedException.expectMessage("Unsupported property key 'unsupportedKey' found.");
	URI uri = new URI("ductiledb:/opt/hbase/conf/hbase-site.xml?unsupportedKey=test");
	new DecodedURI(uri);
    }

    @Test
    public void testInvalidURIWithUnsupportPropertyFormat() throws URISyntaxException {
	expectedException.expect(XOException.class);
	expectedException.expectMessage("Unsupported property format 'namespace' found.");
	URI uri = new URI("ductiledb:/opt/hbase/conf/hbase-site.xml?namespace");
	new DecodedURI(uri);
    }
}