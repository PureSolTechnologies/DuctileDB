package com.puresoltechnologies.ductiledb.xo.test.bootstrap;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.Query;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.xo.test.AbstractXODuctileDBTest;
import com.puresoltechnologies.ductiledb.xo.test.DuctileDBTestUtils;

@RunWith(Parameterized.class)
public class DuctileDBStoreBootstrapIT extends AbstractXODuctileDBTest {

    public DuctileDBStoreBootstrapIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getCdoUnits() throws IOException {
	return DuctileDBTestUtils.configuredXOUnits();
    }

    @Test
    @Ignore("Queries are not implemented, yet.")
    public void bootstrap() {
	XOManager xoManager = getXOManager();

	xoManager.currentTransaction().begin();
	TestEntity a = xoManager.create(TestEntity.class);
	a.setName("Test");
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	Query<TestEntity> query = xoManager.createQuery("_().has('name','Test')", TestEntity.class);
	TestEntity readA = query.execute().getSingleResult();
	assertEquals(a.getName(), readA.getName());
	xoManager.currentTransaction().commit();
    }

}
