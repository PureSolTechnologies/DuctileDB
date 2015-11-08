package com.puresoltechnologies.xo.titan.test;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.XOManagerFactory;
import com.buschmais.xo.api.bootstrap.XO;
import com.buschmais.xo.api.bootstrap.XOUnit;

/**
 * This is an abstract parent class for XO-Titan tests.
 * 
 * @author Rick-Rainer Ludwig
 */
public abstract class AbstractXOTitanTest {

	private XOManagerFactory xoManagerFactory;
	private XOManager xoManager;

	private final XOUnit xoUnit;

	public AbstractXOTitanTest(XOUnit xoUnit) {
		super();
		this.xoUnit = xoUnit;
	}

	@BeforeClass
	public static void dropTitanKeyspace() {
	}

	@Before
	public final void setup() {
		XOTitanTestUtils.dropTitanKeyspace("localhost", "titantest");
		xoManagerFactory = XO.createXOManagerFactory(xoUnit);
		xoManager = xoManagerFactory.createXOManager();
		XOTitanTestUtils.clearTitanKeyspace(xoUnit);
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
