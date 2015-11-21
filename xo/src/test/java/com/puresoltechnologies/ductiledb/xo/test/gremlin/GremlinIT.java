package com.puresoltechnologies.ductiledb.xo.test.gremlin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.Query;
import com.buschmais.xo.api.Query.Result;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.xo.test.AbstractXOTitanTest;
import com.puresoltechnologies.ductiledb.xo.test.DuctileDBTestUtils;
import com.puresoltechnologies.ductiledb.xo.test.data.Person;

@RunWith(Parameterized.class)
public class GremlinIT extends AbstractXOTitanTest {

    public GremlinIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getCdoUnits() throws IOException {
	return DuctileDBTestUtils.configuredXOUnits();
    }

    @Before
    public void setupData() throws IOException {
	DuctileDBTestUtils.addStarwarsData(getXOManager());
    }

    @Test
    public void findSkywalkerFamily() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();

	Query<Person> query = xoManager.createQuery("_().has('lastName', 'Skywalker')", Person.class);
	assertNotNull(query);

	Result<Person> results = query.execute();
	assertNotNull(results);

	int count = 0;
	for (Person person : results) {
	    count++;
	    assertEquals("Skywalker", person.getLastName());
	}
	assertEquals(4, count);

	xoManager.currentTransaction().commit();
    }
}
