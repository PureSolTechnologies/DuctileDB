package com.puresoltechnologies.xo.titan.test.delegate;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map;

import org.hamcrest.collection.IsMapContaining;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.CompositeObject;
import com.buschmais.xo.api.Query;
import com.buschmais.xo.api.Query.Result;
import com.buschmais.xo.api.Query.Result.CompositeRowObject;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileDBStoreSession;
import com.puresoltechnologies.xo.titan.test.AbstractXOTitanTest;
import com.puresoltechnologies.xo.titan.test.XOTitanTestUtils;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

@RunWith(Parameterized.class)
public class DelegateIT extends AbstractXOTitanTest {

	public DelegateIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getCdoUnits() throws URISyntaxException {
		return XOTitanTestUtils.xoUnits(A.class, B.class, A2B.class);
	}

	@Test
	public void entity() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		Vertex node = ((CompositeObject) xoManager.create(A.class))
				.getDelegate();
		assertThat(
				node.<String> getProperty(DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY
						+ "A"), equalTo("A"));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void relation() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		B b = xoManager.create(B.class);
		xoManager.create(a, A2B.class, b);
		Query<A2B> query = xoManager.createQuery("_().has('"
				+ DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY + "A').outE",
				A2B.class);
		Result<A2B> result = query.execute();
		A2B a2b = result.getSingleResult();
		CompositeObject composite = (CompositeObject) a2b;
		Edge edge = composite.getDelegate();
		assertThat(edge.getLabel(), equalTo("RELATION"));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void row() {
		XOManager xoManager = getXOManager();

		xoManager.currentTransaction().begin();
		xoManager.create(A.class);
		Query<CompositeRowObject> query = xoManager.createQuery("_().has('"
				+ DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY + "A').map");
		Result<CompositeRowObject> row = query.execute();
		Map<String, Object> delegate = row.getSingleResult().getDelegate();
		assertThat(delegate, IsMapContaining.<String, Object> hasEntry(
				DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY + "A", "A"));
		xoManager.currentTransaction().commit();
	}
}
