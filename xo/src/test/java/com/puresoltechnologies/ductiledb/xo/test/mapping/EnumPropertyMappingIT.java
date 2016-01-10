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
public class EnumPropertyMappingIT extends AbstractXODuctileDBTest {

    public EnumPropertyMappingIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getCdoUnits() throws IOException {
	return DuctileDBTestUtils.xoUnits(A.class);
    }

    @Test
    public void enumerationLabel() {
	XOManager xoManager = getXOManager();

	xoManager.currentTransaction().begin();
	A a = xoManager.create(A.class);
	a.setEnumeration(Enumeration.FIRST);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	assertThat(a.getEnumeration(), equalTo(Enumeration.FIRST));
	Query<CompositeRowObject> query = xoManager
		.createQuery("_().has('_xo_discriminator_A').has('enumeration','FIRST').map");
	CompositeRowObject result = query.execute().getSingleResult();
	assertThat(result.get("_xo_discriminator_A", String.class), is("A"));
	a.setEnumeration(Enumeration.SECOND);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	assertThat(a.getEnumeration(), equalTo(Enumeration.SECOND));
	query = xoManager.createQuery("_().has('_xo_discriminator_A').has('enumeration','SECOND').map");
	result = query.execute().getSingleResult();
	assertThat(result.get("_xo_discriminator_A", String.class), is("A"));
	a.setEnumeration(null);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	assertThat(a.getEnumeration(), equalTo(null));
	xoManager.currentTransaction().commit();
    }

    @Test
    public void enumerationProperty() {
	XOManager xoManager = getXOManager();

	xoManager.currentTransaction().begin();
	A a = xoManager.create(A.class);
	a.setMappedEnumeration(Enumeration.FIRST);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	assertThat(a.getMappedEnumeration(), equalTo(Enumeration.FIRST));
	Query<CompositeRowObject> query = xoManager
		.createQuery("_().hasLabel('A').has('MAPPED_ENUMERATION','FIRST').map");
	CompositeRowObject result = query.execute().getSingleResult();
	// assertThat(
	// executeQuery(
	// "MATCH (a:A) WHERE a.MAPPED_ENUMERATION='FIRST' RETURN a")
	// .getColumn("a"), hasItem(a));
	assertThat(result.get("_xo_discriminator_A", String.class), is("A"));
	a.setMappedEnumeration(Enumeration.SECOND);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	assertThat(a.getMappedEnumeration(), equalTo(Enumeration.SECOND));
	query = xoManager.createQuery("_().hasLabel('A').has('MAPPED_ENUMERATION','SECOND').map");
	result = query.execute().getSingleResult();
	// assertThat(
	// executeQuery(
	// "MATCH (a:A) WHERE a.MAPPED_ENUMERATION='SECOND' RETURN a")
	// .getColumn("a"), hasItem(a));
	assertThat(result.get("_xo_discriminator_A", String.class), is("A"));
	a.setMappedEnumeration(null);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	assertThat(a.getMappedEnumeration(), equalTo(null));
	xoManager.currentTransaction().commit();
    }
}
