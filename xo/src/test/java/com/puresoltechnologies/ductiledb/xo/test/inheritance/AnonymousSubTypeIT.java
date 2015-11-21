package com.puresoltechnologies.ductiledb.xo.test.inheritance;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.net.URISyntaxException;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.xo.test.AbstractXOTitanTest;
import com.puresoltechnologies.ductiledb.xo.test.DuctileDBTestUtils;

@RunWith(Parameterized.class)
public class AnonymousSubTypeIT extends AbstractXOTitanTest {

	public AnonymousSubTypeIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getCdoUnits() throws URISyntaxException {
		return DuctileDBTestUtils.xoUnits(D.class);
	}

	@Test
	public void anonymousSubType() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		D b = xoManager.create(D.class);
		b.setIndex("1");
		xoManager.currentTransaction().commit();

		xoManager.currentTransaction().begin();
		A a = xoManager.find(A.class, "1").iterator().next();
		assertThat(a.getIndex(), equalTo("1"));
		xoManager.currentTransaction().commit();
	}

}
