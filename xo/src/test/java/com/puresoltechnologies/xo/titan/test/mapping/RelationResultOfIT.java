package com.puresoltechnologies.xo.titan.test.mapping;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
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
public class RelationResultOfIT extends AbstractXOTitanTest {

	private E e;
	private F f1;
	private F f2;

	private E2F e2f1;
	private E2F e2f2;

	public RelationResultOfIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getCdoUnits() throws URISyntaxException {
		return XOTitanTestUtils.xoUnits(E.class, F.class, E2F.class);
	}

	@Before
	public void createData() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		e = xoManager.create(E.class);
		f1 = xoManager.create(F.class);
		e2f1 = xoManager.create(e, E2F.class, f1);
		e2f1.setValue("E2F1");
		f2 = xoManager.create(F.class);
		e2f2 = xoManager.create(e, E2F.class, f2);
		e2f2.setValue("E2F2");
		xoManager.currentTransaction().commit();
	}

	@Test
	public void resultUsingExplicitQuery() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		Result<E2F.ByValue> byValue = e2f1
				.getResultByValueUsingExplicitQuery("E2F1");
		assertThat(byValue.getSingleResult().getF(), equalTo(f1));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void resultUsingReturnType() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		Result<E2F.ByValue> byValue = e2f1
				.getResultByValueUsingReturnType("E2F1");
		assertThat(byValue.getSingleResult().getF(), equalTo(f1));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void byValueUsingExplicitQuery() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		E2F.ByValue byValue = e2f1.getByValueUsingExplicitQuery("E2F1");
		assertThat(byValue.getF(), equalTo(f1));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void byValueUsingReturnType() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		E2F.ByValue byValue = e2f1.getByValueUsingReturnType("E2F1");
		assertThat(byValue.getF(), equalTo(f1));
		byValue = e2f1.getByValueUsingReturnType("unknownE2F");
		assertThat(byValue, equalTo(null));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void byValueUsingImplicitThis() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		E2F.ByValueUsingImplicitThis byValue = e2f1
				.getByValueUsingImplicitThis("E2F1");
		assertThat(byValue.getF(), equalTo(f1));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void resultUsingGremlin() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		Result<F> result = e2f1.getResultUsingGremlin("E2F1");
		assertThat(result, hasItems(equalTo(f1)));
		result = e2f1.getResultUsingGremlin("unknownF");
		assertThat(result.iterator().hasNext(), equalTo(false));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void singleResultUsingGremlin() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		F result = e2f1.getSingleResultUsingGremlin("E2F1");
		assertThat(result, equalTo(f1));
		result = e2f1.getSingleResultUsingGremlin("unknownF");
		assertThat(result, equalTo(null));
		xoManager.currentTransaction().commit();
	}

}
