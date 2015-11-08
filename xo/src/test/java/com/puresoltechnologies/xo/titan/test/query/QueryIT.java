package com.puresoltechnologies.xo.titan.test.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.fail;

import java.net.URISyntaxException;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.Query.Result;
import com.buschmais.xo.api.Query.Result.CompositeRowObject;
import com.buschmais.xo.api.XOException;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.xo.titan.test.AbstractXOTitanTest;
import com.puresoltechnologies.xo.titan.test.XOTitanTestUtils;

@RunWith(Parameterized.class)
public class QueryIT extends AbstractXOTitanTest {

	private A a1;
	private A a2_1;
	private A a2_2;

	public QueryIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getXOUnits() throws URISyntaxException {
		return XOTitanTestUtils.xoUnits(A.class, B.class, A2B.class);
	}

	@Before
	public void createData() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		a1 = xoManager.create(A.class);
		a1.setValue("A1");
		a2_1 = xoManager.create(A.class);
		a2_1.setValue("A2");
		a2_2 = xoManager.create(A.class);
		a2_2.setValue("A2");
		xoManager.currentTransaction().commit();
	}

	@Test
	public void gremlinStringQuery() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		Result<CompositeRowObject> result = xoManager
				.createQuery(
						"_().has('_xo_discriminator_A').has('value', {value})")
				.withParameter("value", "A1").execute();
		A a = result.getSingleResult().get("unknown", A.class);
		assertThat(a.getValue(), equalTo("A1"));
		result = xoManager
				.createQuery(
						"_().has('_xo_discriminator_A').has('value', {value})")
				.withParameter("value", "A2").execute();
		try {
			result.getSingleResult().get("a", A.class);
			fail("Expecting a " + XOException.class.getName());
		} catch (XOException e) {
		}
		xoManager.currentTransaction().commit();
	}

	@Test
	public void gremlinStringQuerySimple() {
		XOManager xoManager = getXOManager();

		xoManager.currentTransaction().begin();
		Result<CompositeRowObject> result = xoManager.createQuery(
				"_().has('_xo_discriminator_A').value").execute();
		for (CompositeRowObject row : result) {
			assertThat(row.get("unknown_type", String.class),
					isOneOf("A1", "A2"));
		}

		xoManager.currentTransaction().commit();
	}

	@Test
	public void compositeRowTypedQuery() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		Result<InstanceByValue> result = xoManager
				.createQuery(InstanceByValue.class)
				.withParameter("value", "A1").execute();
		A a = result.getSingleResult().getA();
		assertThat(a.getValue(), equalTo("A1"));
		result = xoManager.createQuery(InstanceByValue.class)
				.withParameter("value", "A2").execute();
		try {
			result.getSingleResult().getA();
			fail("Expecting a " + XOException.class.getName());
		} catch (XOException e) {
		}
		xoManager.currentTransaction().commit();
	}

	@Test
	public void typedQuery() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		Result<InstanceByValue> result = xoManager
				.createQuery(InstanceByValue.class)
				.withParameter("value", "A1").execute();
		A a = result.getSingleResult().getA();
		assertThat(a.getValue(), equalTo("A1"));
		xoManager.currentTransaction().commit();
	}

}
