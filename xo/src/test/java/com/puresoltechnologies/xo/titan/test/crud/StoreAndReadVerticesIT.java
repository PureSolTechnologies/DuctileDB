package com.puresoltechnologies.xo.titan.test.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.ResultIterable;
import com.buschmais.xo.api.ResultIterator;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.xo.titan.test.AbstractXOTitanTest;
import com.puresoltechnologies.xo.titan.test.XOTitanTestUtils;
import com.puresoltechnologies.xo.titan.test.bootstrap.TestEntity;

@RunWith(Parameterized.class)
public class StoreAndReadVerticesIT extends AbstractXOTitanTest {

    public StoreAndReadVerticesIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getCdoUnits() throws IOException {
	return XOTitanTestUtils.configuredXOUnits();
    }

    @Test
    public void test() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	TestEntity createdA = xoManager.create(TestEntity.class);
	createdA.setName("Test");
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	ResultIterable<TestEntity> aa = xoManager
		.find(TestEntity.class, "Test");
	assertNotNull(aa);
	ResultIterator<TestEntity> iterator = aa.iterator();
	assertTrue(iterator.hasNext());
	TestEntity readA = iterator.next();
	assertNotNull(readA);
	assertEquals("Test", readA.getName());
	xoManager.currentTransaction().rollback();
    }

}
