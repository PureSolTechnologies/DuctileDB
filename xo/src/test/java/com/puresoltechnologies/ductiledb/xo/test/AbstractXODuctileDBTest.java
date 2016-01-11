package com.puresoltechnologies.ductiledb.xo.test;

import org.junit.After;
import org.junit.Before;

import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.XOManagerFactory;
import com.buschmais.xo.api.bootstrap.XO;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBGraphTest;

/**
 * This is an abstract parent class for XO-DuctileDB tests.
 * 
 * @author Rick-Rainer Ludwig
 */
public abstract class AbstractXODuctileDBTest extends AbstractDuctileDBGraphTest {

    private XOManagerFactory xoManagerFactory;
    private XOManager xoManager;

    private final XOUnit xoUnit;

    public AbstractXODuctileDBTest(XOUnit xoUnit) {
	super();
	this.xoUnit = xoUnit;
    }

    @Before
    public final void setup() {
	xoManagerFactory = XO.createXOManagerFactory(xoUnit);
	xoManager = xoManagerFactory.createXOManager();
    }

    @After
    public final void destroy() {
	if (xoManager != null) {
	    xoManager.close();
	}
	if (xoManagerFactory != null) {
	    xoManagerFactory.close();
	}
    }

    public XOManagerFactory getXOManagerFactory() {
	return xoManagerFactory;
    }

    public XOManager getXOManager() {
	if (xoManager == null) {
	    xoManager = xoManagerFactory.createXOManager();
	}
	return xoManager;
    }

    public void closeXOManager() {
	xoManager.close();
	xoManager = null;
    }
}
