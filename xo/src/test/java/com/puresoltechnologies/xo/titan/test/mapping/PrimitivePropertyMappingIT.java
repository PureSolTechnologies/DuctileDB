package com.puresoltechnologies.xo.titan.test.mapping;

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
import com.puresoltechnologies.xo.titan.test.AbstractXOTitanTest;
import com.puresoltechnologies.xo.titan.test.XOTitanTestUtils;

@RunWith(Parameterized.class)
public class PrimitivePropertyMappingIT extends AbstractXOTitanTest {

	public PrimitivePropertyMappingIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getCdoUnits() throws URISyntaxException {
		return XOTitanTestUtils.xoUnits(A.class);
	}

	@Test
	public void primitiveProperty() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		a.setString("value");
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		assertThat(a.getString(), equalTo("value"));
		a.setString("updatedValue");
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		assertThat(a.getString(), equalTo("updatedValue"));
		a.setString(null);
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		assertThat(a.getString(), equalTo(null));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void mappedPrimitiveProperty() {
		XOManager xoManager = getXOManager();

		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		a.setMappedString("mappedValue");
		xoManager.currentTransaction().commit();

		xoManager.currentTransaction().begin();
		Query<CompositeRowObject> query = xoManager
				.createQuery("_().has('_xo_discriminator_A').map");
		CompositeRowObject result = query.execute().getSingleResult();
		// TestResult result =
		// executeQuery("match (a:A) return a.MAPPED_STRING as v");
		assertThat(result.get("MAPPED_STRING", String.class), is("mappedValue"));
		xoManager.currentTransaction().commit();
	}
}
