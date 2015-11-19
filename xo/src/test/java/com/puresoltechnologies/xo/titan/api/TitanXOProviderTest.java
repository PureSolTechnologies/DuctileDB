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
import com.puresoltechnologies.ductiledb.xo.api.DuctileDBXOProvider;

public class TitanXOProviderTest {

    private static DuctileDBXOProvider ductileDBXOProvider = null;

    @BeforeClass
    public static void initialize() {
	ductileDBXOProvider = new DuctileDBXOProvider();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullXOUnit() {
	ductileDBXOProvider.createDatastore(null);
    }

    @Test(expected = XOException.class)
    public void testNullURI() {
	XOUnit xoUnit = Mockito.mock(XOUnit.class);
	when(xoUnit.getUri()).thenReturn(null);
	ductileDBXOProvider.createDatastore(xoUnit);
    }

    @Test(expected = XOException.class)
    public void testIllegalProtocol() throws URISyntaxException {
	XOUnit xoUnit = mock(XOUnit.class);
	URI uri = new URI("illegal-ductiledb://titan");
	when(xoUnit.getUri()).thenReturn(uri);
	ductileDBXOProvider.createDatastore(xoUnit);
    }
}
