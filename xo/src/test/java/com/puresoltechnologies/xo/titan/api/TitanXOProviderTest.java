package com.puresoltechnologies.xo.titan.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.xo.api.TitanXOProvider;

public class TitanXOProviderTest {

	private static TitanXOProvider titanXOProvider = null;

	@BeforeClass
	public static void initialize() {
		titanXOProvider = new TitanXOProvider();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullXOUnit() {
		titanXOProvider.createDatastore(null);
	}

	@Test(expected = XOException.class)
	public void testNullURI() {
		XOUnit xoUnit = Mockito.mock(XOUnit.class);
		when(xoUnit.getUri()).thenReturn(null);
		titanXOProvider.createDatastore(xoUnit);
	}

	@Test(expected = XOException.class)
	public void testIllegalProtocol() throws URISyntaxException {
		XOUnit xoUnit = mock(XOUnit.class);
		URI uri = new URI("illegal-titan-cassandra://titan");
		when(xoUnit.getUri()).thenReturn(uri);
		titanXOProvider.createDatastore(xoUnit);
	}
}
