package com.puresoltechnologies.xo.titan.test.transaction;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.ConcurrencyMode;
import com.buschmais.xo.api.Transaction;
import com.buschmais.xo.api.ValidationMode;
import com.buschmais.xo.api.XOException;
import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.xo.titan.test.AbstractXOTitanTest;
import com.puresoltechnologies.xo.titan.test.XOTitanTestUtils;

@RunWith(Parameterized.class)
public class TransactionAttributeMandatoryIT extends AbstractXOTitanTest {

	public TransactionAttributeMandatoryIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getXOUnits() throws URISyntaxException {
		return XOTitanTestUtils.xoUnits(Arrays.asList(A.class),
				Collections.<Class<?>> emptyList(), ValidationMode.AUTO,
				ConcurrencyMode.SINGLETHREADED,
				Transaction.TransactionAttribute.MANDATORY);
	}

	@Test
	public void withoutTransactionContext() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		a.setValue("value1");
		xoManager.currentTransaction().commit();
		assertThat(xoManager.currentTransaction().isActive(), equalTo(false));
		try {
			a.getValue();
			Assert.fail("A XOException is expected.");
		} catch (XOException e) {
		}
		try {
			a.setValue("value2");
			Assert.fail("A XOException is expected.");
		} catch (XOException e) {
		}
	}
}
