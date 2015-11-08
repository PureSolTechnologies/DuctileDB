package com.puresoltechnologies.xo.titan.test.relation.implicit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.net.URISyntaxException;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.Query;
import com.buschmais.xo.api.Query.Result.CompositeRowObject;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileDBStoreSession;
import com.puresoltechnologies.xo.titan.test.AbstractXOTitanTest;
import com.puresoltechnologies.xo.titan.test.XOTitanTestUtils;

@RunWith(Parameterized.class)
public class ImplicitRelationIT extends AbstractXOTitanTest {

	public ImplicitRelationIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getXOUnits() throws URISyntaxException {
		return XOTitanTestUtils.xoUnits(A.class, B.class);
	}

	@Test
	public void oneToOne() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		B b1 = xoManager.create(B.class);
		a.setOneToOne(b1);
		xoManager.currentTransaction().commit();

		xoManager.currentTransaction().begin();
		assertThat(a.getOneToOne(), equalTo(b1));
		assertThat(b1.getOneToOne(), equalTo(a));
		// Query<CompositeRowObject> query = xoManager
		// .createQuery("MATCH (a:A)-[:ImplicitOneToOne]->(b:B) RETURN b");
		Query<CompositeRowObject> query = xoManager
				.createQuery("_().has('"
						+ DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY
						+ "A').outE.inV");
		CompositeRowObject result = query.execute().getSingleResult();
		assertThat(result.get(result.getColumns().iterator().next(), B.class),
				is(b1));
		B b2 = xoManager.create(B.class);
		a.setOneToOne(b2);
		xoManager.currentTransaction().commit();

		xoManager.currentTransaction().begin();
		assertThat(a.getOneToOne(), equalTo(b2));
		assertThat(b2.getOneToOne(), equalTo(a));
		assertThat(b1.getOneToOne(), equalTo(null));
		// query = xoManager
		// .createQuery("MATCH (a:A)-[:ImplicitOneToOne]->(b:B) RETURN b");
		query = xoManager
				.createQuery("_().has('"
						+ DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY
						+ "A').outE.inV");
		result = query.execute().getSingleResult();
		assertThat(result.get(result.getColumns().iterator().next(), B.class),
				is(b2));
		a.setOneToOne(null);
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		assertThat(a.getOneToOne(), equalTo(null));
		assertThat(b1.getOneToOne(), equalTo(null));
		assertThat(b2.getOneToOne(), equalTo(null));
		xoManager.currentTransaction().commit();
	}
}
