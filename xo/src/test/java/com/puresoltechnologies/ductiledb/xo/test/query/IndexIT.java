package com.puresoltechnologies.ductiledb.xo.test.query;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.xo.test.AbstractXODuctileDBTest;
import com.puresoltechnologies.ductiledb.xo.test.DuctileDBTestUtils;

@RunWith(Parameterized.class)
public class IndexIT extends AbstractXODuctileDBTest {

    public IndexIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getCdoUnits() throws IOException {
	return DuctileDBTestUtils.xoUnits(A.class);
    }

    @Test
    public void index() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a1 = xoManager.create(A.class);
	a1.setValue("A1");
	A a2_1 = xoManager.create(A.class);
	a2_1.setValue("A2");
	A a2_2 = xoManager.create(A.class);
	a2_2.setValue("A2");
	xoManager.currentTransaction().commit();
	xoManager.currentTransaction().begin();
	A a = xoManager.find(A.class, "A1").getSingleResult();
	assertThat(a, equalTo(a1));
	try {
	    xoManager.find(A.class, "A2").getSingleResult();
	    fail("Expecting a " + XOException.class.getName());
	} catch (XOException e) {

	}
	xoManager.currentTransaction().commit();
    }
}
