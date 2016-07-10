package com.puresoltechnologies.ductiledb.xo.test.query;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;

import javax.naming.directory.SchemaViolationException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.api.graph.schema.DuctileDBUniqueConstraintViolationException;
import com.puresoltechnologies.ductiledb.xo.test.AbstractXODuctileDBTest;
import com.puresoltechnologies.ductiledb.xo.test.DuctileDBTestUtils;

@RunWith(Parameterized.class)
public class UniqueIT extends AbstractXODuctileDBTest {

    public UniqueIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getCdoUnits() throws IOException {
	return DuctileDBTestUtils.xoUnits(B.class);
    }

    /**
     * This test checks for the presence of a unique constraint. DuctileDB
     * provides now a dedicated unique constraint violation exception:
     * {@link SchemaViolationException}. For this exception it is checked here.
     */
    @Test(expected = DuctileDBUniqueConstraintViolationException.class)
    public void denyDuplicates() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	B a1 = xoManager.create(B.class);
	a1.setValue("A1");
	B a2_1 = xoManager.create(B.class);
	a2_1.setValue("A2");
	B a2_2 = xoManager.create(B.class);
	a2_2.setValue("A2");
	xoManager.currentTransaction().commit();
    }

    @Test
    public void index() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	B a1 = xoManager.create(B.class);
	a1.setValue("A1");
	B a2_1 = xoManager.create(B.class);
	a2_1.setValue("A2");
	xoManager.currentTransaction().commit();
	xoManager.currentTransaction().begin();
	B a = xoManager.find(B.class, "A1").getSingleResult();
	assertThat(a, equalTo(a1));
	try {
	    xoManager.find(B.class, "A3").getSingleResult();
	    fail("Expecting a " + XOException.class.getName());
	} catch (XOException e) {

	}
	xoManager.currentTransaction().commit();
    }
}
