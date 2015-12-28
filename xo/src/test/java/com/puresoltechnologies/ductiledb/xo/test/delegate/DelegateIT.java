package com.puresoltechnologies.ductiledb.xo.test.delegate;

import java.net.URISyntaxException;
import java.util.Collection;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.CompositeObject;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertex;
import com.puresoltechnologies.ductiledb.xo.test.AbstractXODuctileDBTest;
import com.puresoltechnologies.ductiledb.xo.test.DuctileDBTestUtils;

@RunWith(Parameterized.class)
public class DelegateIT extends AbstractXODuctileDBTest {

    public DelegateIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getCdoUnits() throws URISyntaxException {
	return DuctileDBTestUtils.xoUnits(A.class, B.class, A2B.class);
    }

    @Test
    public void entity() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	DuctileVertex node = ((CompositeObject) xoManager.create(A.class)).getDelegate();
	xoManager.currentTransaction().commit();
    }

    @Test
    @Ignore("Queries are not supported, yet.")
    public void relation() {
	// XOManager xoManager = getXOManager();
	// xoManager.currentTransaction().begin();
	// A a = xoManager.create(A.class);
	// B b = xoManager.create(B.class);
	// xoManager.create(a, A2B.class, b);
	// Query<A2B> query = xoManager
	// .createQuery("_().has('" +
	// DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY + "A').outE",
	// A2B.class);
	// Result<A2B> result = query.execute();
	// A2B a2b = result.getSingleResult();
	// CompositeObject composite = (CompositeObject) a2b;
	// Edge edge = composite.getDelegate();
	// assertThat(edge.getLabel(), equalTo("RELATION"));
	// xoManager.currentTransaction().commit();
    }

    @Test
    @Ignore("Queries are not supported, yet.")
    public void row() {
	// XOManager xoManager = getXOManager();
	//
	// xoManager.currentTransaction().begin();
	// xoManager.create(A.class);
	// Query<CompositeRowObject> query = xoManager
	// .createQuery("_().has('" +
	// DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY + "A').map");
	// Result<CompositeRowObject> row = query.execute();
	// Map<String, Object> delegate = row.getSingleResult().getDelegate();
	// assertThat(delegate,
	// IsMapContaining.<String, Object>
	// hasEntry(DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY + "A",
	// "A"));
	// xoManager.currentTransaction().commit();
    }
}
