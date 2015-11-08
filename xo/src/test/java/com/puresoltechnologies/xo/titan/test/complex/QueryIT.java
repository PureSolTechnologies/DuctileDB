package com.puresoltechnologies.xo.titan.test.complex;

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
import com.buschmais.xo.api.ResultIterable;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.xo.titan.test.AbstractXOTitanTest;
import com.puresoltechnologies.xo.titan.test.XOTitanTestUtils;
import com.puresoltechnologies.xo.titan.test.data.Person;

@RunWith(Parameterized.class)
public class QueryIT extends AbstractXOTitanTest {

	public QueryIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getCdoUnits() throws IOException {
		return XOTitanTestUtils.configuredXOUnits();
	}

	@Before
	public void setupData() {
		XOTitanTestUtils.addStarwarsData(getXOManager());
	}

	@Test
	public void test() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		ResultIterable<Person> people = xoManager.find(Person.class,
				"Skywalker");
		assertNotNull(people);

		int count = 0;
		for (Person person : people) {
			count++;
		}
		assertEquals(4, count);

		xoManager.currentTransaction().commit();
	}

	@Test
	public void testRelations() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();

		Query<Person> query = xoManager.createQuery(
				"_().has('lastName', 'Skywalker').has('firstName','Luke')",
				Person.class);
		Result<Person> result = query.execute();
		Person lukeSkywalker = result.getSingleResult();
		assertEquals("Luke", lukeSkywalker.getFirstName());
		assertEquals("Skywalker", lukeSkywalker.getLastName());

		Person anakinSkywalker = lukeSkywalker.getFather();
		assertNotNull(anakinSkywalker);
		assertEquals("Anakin", anakinSkywalker.getFirstName());
		assertEquals("Skywalker", anakinSkywalker.getLastName());

		Person leaSkywalker = lukeSkywalker.getMother();
		assertNotNull(leaSkywalker);
		assertEquals("Padme", leaSkywalker.getFirstName());
		assertEquals("Skywalker", leaSkywalker.getLastName());

		xoManager.currentTransaction().commit();
	}

}
