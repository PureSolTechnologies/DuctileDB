package com.puresoltechnologies.ductiledb.xo.test.implementedby;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.net.URISyntaxException;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.xo.test.AbstractXODuctileDBTest;
import com.puresoltechnologies.ductiledb.xo.test.DuctileDBTestUtils;

@RunWith(Parameterized.class)
public class RelationImplementedByIT extends AbstractXODuctileDBTest {

	public RelationImplementedByIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getCdoUnits() throws URISyntaxException {
		return DuctileDBTestUtils.xoUnits(A.class, B.class, A2B.class);
	}

	@Test
	public void nonPropertyMethod() {
		XOManager xoManager = getXOManagerFactory().createXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		B b = xoManager.create(B.class);
		A2B a2b = xoManager.create(a, A2B.class, b);
		a2b.setValue(1);
		int i = a2b.incrementValue();
		assertThat(i, equalTo(2));
		xoManager.currentTransaction().commit();
		xoManager.close();
	}

	@Test
	public void propertyMethods() {
		XOManager xoManager = getXOManagerFactory().createXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		B b = xoManager.create(B.class);
		A2B a2b = xoManager.create(a, A2B.class, b);
		a2b.setCustomValue("VALUE");
		String value = a2b.getCustomValue();
		assertThat(value, equalTo("set_VALUE_get"));
		xoManager.currentTransaction().commit();
		xoManager.close();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void unsupportedOperation() {
		XOManager xoManager = getXOManagerFactory().createXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		B b = xoManager.create(B.class);
		A2B a2b = xoManager.create(a, A2B.class, b);
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		try {
			a2b.unsupportedOperation();
		} finally {
			xoManager.currentTransaction().commit();
		}
	}
}
