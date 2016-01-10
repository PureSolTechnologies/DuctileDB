package com.puresoltechnologies.ductiledb.xo.test.mapping;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.xo.test.AbstractXODuctileDBTest;
import com.puresoltechnologies.ductiledb.xo.test.DuctileDBTestUtils;

@RunWith(Parameterized.class)
public class IndexMappingIT extends AbstractXODuctileDBTest {

    public IndexMappingIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getCdoUnits() throws IOException {
	return DuctileDBTestUtils.xoUnits(A.class, D.class);
    }

    @Test
    public void indexedProperty() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a1 = xoManager.create(A.class);
	a1.setIndex("1");
	A a2 = xoManager.create(A.class);
	a2.setIndex("2");
	xoManager.currentTransaction().commit();
	xoManager.currentTransaction().begin();
	assertThat(xoManager.find(A.class, "1").iterator().next(), equalTo(a1));
	assertThat(xoManager.find(A.class, "2").iterator().next(), equalTo(a2));
	assertThat(xoManager.find(A.class, "3").iterator().hasNext(), equalTo(false));
	xoManager.currentTransaction().commit();
    }

    @Test
    public void useIndexOf() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a1 = xoManager.create(D.class);
	a1.setIndex("1");
	xoManager.currentTransaction().commit();
	xoManager.currentTransaction().begin();
	assertThat(xoManager.find(D.class, "1").iterator().next(), equalTo(a1));
	xoManager.currentTransaction().commit();
    }
}
