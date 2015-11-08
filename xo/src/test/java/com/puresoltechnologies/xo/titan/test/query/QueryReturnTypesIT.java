package com.puresoltechnologies.xo.titan.test.query;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.net.URISyntaxException;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.Query.Result;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.xo.titan.test.AbstractXOTitanTest;
import com.puresoltechnologies.xo.titan.test.XOTitanTestUtils;

@RunWith(Parameterized.class)
public class QueryReturnTypesIT extends AbstractXOTitanTest {

	private A a;

	public QueryReturnTypesIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getXOUnits() throws URISyntaxException {
		return XOTitanTestUtils.xoUnits(A.class);
	}

	@Before
	public void createData() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		a = xoManager.create(A.class);
		a.setValue("A");
		xoManager.currentTransaction().commit();
	}

	@Test
	public void gremlinWithPrimitiveReturnType() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		Result<String> result = xoManager.createQuery(
				"_().has('_xo_discriminator_A').value", String.class).execute();
		assertThat(result.getSingleResult(), equalTo("A"));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void gremlinWithEntityReturnType() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		Result<A> result = xoManager.createQuery(
				"_().has('_xo_discriminator_A')", A.class).execute();
		assertThat(result.getSingleResult(), equalTo(a));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void typedQuery() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		Result<InstanceByValue> result = xoManager
				.createQuery(InstanceByValue.class).withParameter("value", "A")
				.execute();
		assertThat(result.getSingleResult().getA(), equalTo(a));
		xoManager.currentTransaction().commit();
	}
}
