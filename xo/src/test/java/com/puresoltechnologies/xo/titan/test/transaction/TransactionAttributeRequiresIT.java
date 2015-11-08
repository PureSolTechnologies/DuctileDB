package com.puresoltechnologies.xo.titan.test.transaction;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.ConcurrencyMode;
import com.buschmais.xo.api.Query;
import com.buschmais.xo.api.Transaction;
import com.buschmais.xo.api.ValidationMode;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.xo.titan.test.AbstractXOTitanTest;
import com.puresoltechnologies.xo.titan.test.XOTitanTestUtils;
import com.puresoltechnologies.xo.titan.test.transaction.A.ByValue;

@RunWith(Parameterized.class)
public class TransactionAttributeRequiresIT extends AbstractXOTitanTest {

    public TransactionAttributeRequiresIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getXOUnits() throws URISyntaxException {
	return XOTitanTestUtils.xoUnits(asList(A.class, B.class),
		Collections.<Class<?>> emptyList(), ValidationMode.AUTO,
		ConcurrencyMode.SINGLETHREADED,
		Transaction.TransactionAttribute.REQUIRES);
    }

    @Test
    public void withoutTransactionContext() {
	XOManager xoManager = getXOManager();
	assertThat(xoManager.currentTransaction().isActive(), equalTo(false));

	A a = createA(xoManager);
	assertThat(a.getValue(), equalTo("value1"));
	assertThat(xoManager.find(A.class, "value1").getSingleResult(),
		equalTo(a));

	Query<ByValue> query = xoManager.createQuery(A.ByValue.class);
	query = query.withParameter("value", "value1");
	ByValue result = query.execute().getSingleResult();
	assertThat(result.getA().getValue(), equalTo(a.getValue()));
	assertThat(a.getByValue("value1").getA(), equalTo(a));
	a.setValue("value2");
	assertThat(a.getValue(), equalTo("value2"));
	assertThat(a.getListOfB().size(), equalTo(2));
	int i = 1;
	for (B b : a.getListOfB()) {
	    assertThat(b.getIntValue(), equalTo(i));
	    i++;
	}
    }

    @Test
    public void withTransactionContext() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a = createA(xoManager);
	xoManager.currentTransaction().commit();
	xoManager.currentTransaction().begin();
	assertThat(xoManager.currentTransaction().isActive(), equalTo(true));
	assertThat(a.getValue(), equalTo("value1"));
	a.setValue("value2");
	xoManager.currentTransaction().commit();
	xoManager.currentTransaction().begin();
	assertThat(xoManager.currentTransaction().isActive(), equalTo(true));
	assertThat(a.getValue(), equalTo("value2"));
	a.setValue("value3");
	xoManager.currentTransaction().rollback();
	assertThat(a.getValue(), equalTo("value2"));
    }

    @Test
    public void commitOnException() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a = createA(xoManager);
	xoManager.currentTransaction().commit();
	assertThat(a.getValue(), equalTo("value1"));
	try {
	    a.throwException("value2");
	    Assert.fail("An Exception is expected.");
	} catch (Exception e) {
	}
	assertThat(xoManager.currentTransaction().isActive(), equalTo(false));
	assertThat(a.getValue(), equalTo("value2"));
    }

    @Test
    public void rollbackOnRuntimeException() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a = createA(xoManager);
	xoManager.currentTransaction().commit();
	assertThat(a.getValue(), equalTo("value1"));
	try {
	    a.throwRuntimeException("value2");
	    fail("A RuntimeException is expected.");
	} catch (RuntimeException e) {
	}
	assertThat(xoManager.currentTransaction().isActive(), equalTo(false));
	assertThat(a.getValue(), equalTo("value1"));
    }

    private A createA(XOManager xoManager) {
	B b1 = xoManager.create(B.class);
	b1.setIntValue(1);
	B b2 = xoManager.create(B.class);
	b2.setIntValue(2);
	A a = xoManager.create(A.class);
	a.setValue("value1");
	a.getListOfB().add(b1);
	a.getListOfB().add(b2);
	return a;
    }
}
