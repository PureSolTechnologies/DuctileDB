package com.puresoltechnologies.ductiledb.xo.test.mapping;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.Query;
import com.buschmais.xo.api.Query.Result.CompositeRowObject;
import com.buschmais.xo.api.ResultIterator;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.xo.test.AbstractXODuctileDBTest;
import com.puresoltechnologies.ductiledb.xo.test.DuctileDBTestUtils;

@RunWith(Parameterized.class)
public class CollectionPropertyMappingIT extends AbstractXODuctileDBTest {

    public CollectionPropertyMappingIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getCdoUnits() throws IOException {
	return DuctileDBTestUtils.xoUnits(A.class, B.class);
    }

    @Test
    public void setProperty() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a = xoManager.create(A.class);
	B b = xoManager.create(B.class);
	Set<B> setOfB = a.getSetOfB();
	assertThat(setOfB.add(b), equalTo(true));
	assertThat(setOfB.add(b), equalTo(false));
	xoManager.currentTransaction().commit();
	xoManager.currentTransaction().begin();
	assertThat(setOfB.size(), equalTo(1));
	assertThat(setOfB.remove(b), equalTo(true));
	assertThat(setOfB.remove(b), equalTo(false));
	xoManager.currentTransaction().commit();
    }

    @Test
    public void mappedSetProperty() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a = xoManager.create(A.class);
	B b = xoManager.create(B.class);
	a.getMappedSetOfB().add(b);
	a.getMappedSetOfB().add(b);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	Query<CompositeRowObject> query = xoManager
		.createQuery("_().hasLabel('A').outE.has('label', 'MAPPED_SET_OF_B').inV.map");
	CompositeRowObject result = query.execute().getSingleResult();
	// TestResult result =
	// executeQuery("match (a:A)-[:MAPPED_SET_OF_B]->(b) return b");
	assertThat(result.get("_xo_discriminator_B", String.class), is("B"));
	xoManager.currentTransaction().commit();
    }

    @Test
    public void listProperty() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a = xoManager.create(A.class);
	B b = xoManager.create(B.class);
	List<B> listOfB = a.getListOfB();
	assertThat(listOfB.add(b), equalTo(true));
	assertThat(listOfB.add(b), equalTo(true));
	xoManager.currentTransaction().commit();
	xoManager.currentTransaction().begin();
	assertThat(listOfB.size(), equalTo(2));
	assertThat(listOfB.remove(b), equalTo(true));
	assertThat(listOfB.remove(b), equalTo(true));
	assertThat(listOfB.remove(b), equalTo(false));
	xoManager.currentTransaction().commit();
    }

    @Test
    public void mappedListProperty() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a = xoManager.create(A.class);
	B b = xoManager.create(B.class);
	a.getMappedListOfB().add(b);
	a.getMappedListOfB().add(b);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	Query<CompositeRowObject> query = xoManager
		.createQuery("_().hasLabel('A').outE.has('label', 'MAPPED_LIST_OF_B').inV.map");
	ResultIterator<CompositeRowObject> result = query.execute().iterator();
	assertTrue(result.hasNext());
	CompositeRowObject result1 = result.next();
	assertTrue(result.hasNext());
	CompositeRowObject result2 = result.next();
	assertFalse(result.hasNext());
	// TestResult result =
	// executeQuery("match (a:A)-[:MAPPED_LIST_OF_B]->(b) return b");
	assertThat(result1.get("_xo_discriminator_B", String.class), is("B"));
	assertThat(result2.get("_xo_discriminator_B", String.class), is("B"));
	xoManager.currentTransaction().commit();
    }
}
