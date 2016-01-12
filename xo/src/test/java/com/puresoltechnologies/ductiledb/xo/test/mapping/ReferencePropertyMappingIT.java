package com.puresoltechnologies.ductiledb.xo.test.mapping;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.Query;
import com.buschmais.xo.api.Query.Result.CompositeRowObject;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.xo.test.AbstractXODuctileDBTest;
import com.puresoltechnologies.ductiledb.xo.test.DuctileDBTestUtils;

@RunWith(Parameterized.class)
public class ReferencePropertyMappingIT extends AbstractXODuctileDBTest {

    public ReferencePropertyMappingIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getCdoUnits() throws IOException {
	return DuctileDBTestUtils.xoUnits(A.class, B.class);
    }

    @Test
    public void referenceProperty() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a = xoManager.create(A.class);
	B b1 = xoManager.create(B.class);
	B b2 = xoManager.create(B.class);
	a.setB(b1);
	xoManager.currentTransaction().commit();
	xoManager.currentTransaction().begin();
	assertThat(a.getB(), equalTo(b1));
	a.setB(b2);
	xoManager.currentTransaction().commit();
	xoManager.currentTransaction().begin();
	assertThat(a.getB(), equalTo(b2));
	a.setB(null);
	xoManager.currentTransaction().commit();
	xoManager.currentTransaction().begin();
	assertThat(a.getB(), equalTo(null));
	xoManager.currentTransaction().commit();
    }

    @Test
    public void mappedReferenceProperty() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a = xoManager.create(A.class);
	B b = xoManager.create(B.class);
	a.setMappedB(b);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	Query<CompositeRowObject> query = xoManager
		.createQuery("g.V().has('_xo_discriminator_A').outE.filter{it.label=='MAPPED_B'}.inV.map");
	CompositeRowObject result = query.execute().getSingleResult();
	// TestResult result =
	// executeQuery("match (a:A)-[:MAPPED_B]->(b) return b");
	assertThat(result.get("_xo_discriminator_B", String.class), is("B"));
	xoManager.currentTransaction().commit();
    }

}
