package com.puresoltechnologies.xo.titan.test.label;

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
public class LabelIT extends AbstractXOTitanTest {

	public LabelIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getCdoUnits() throws URISyntaxException {
		return XOTitanTestUtils.xoUnits(ImplicitLabel.class,
				ExplicitLabel.class);
	}

	@Test
	public void implicitLabel() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		xoManager.create(ImplicitLabel.class);
		Query<CompositeRowObject> query = xoManager.createQuery("_().map");
		CompositeRowObject result = query.execute().getSingleResult();
		assertThat(result.get("_xo_discriminator_ImplicitLabel", String.class),
				is("ImplicitLabel"));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void explicitLabel() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		xoManager.create(ExplicitLabel.class);
		Query<CompositeRowObject> query = xoManager.createQuery("_().map");
		CompositeRowObject result = query.execute().getSingleResult();
		assertThat(
				result.get("_xo_discriminator_EXPLICIT_LABEL", String.class),
				is("EXPLICIT_LABEL"));
		xoManager.currentTransaction().commit();
	}
}
