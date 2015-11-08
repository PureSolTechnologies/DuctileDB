package com.puresoltechnologies.xo.titan.test.migration;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.net.URISyntaxException;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.xo.titan.test.AbstractXOTitanTest;
import com.puresoltechnologies.xo.titan.test.XOTitanTestUtils;

@RunWith(Parameterized.class)
public class MigrationIT extends AbstractXOTitanTest {

	public MigrationIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getCdoUnits() throws URISyntaxException {
		return XOTitanTestUtils.xoUnits(A.class, B.class, C.class, D.class);
	}

	@Test
	public void downCast() {
		XOManager xoManager = getXOManagerFactory().createXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		a.setValue("Value");
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		B b = xoManager.migrate(a, B.class);
		assertThat(a == b, equalTo(false));
		assertThat(b.getValue(), equalTo("Value"));
		xoManager.currentTransaction().commit();
		xoManager.close();
	}

	@Test
	public void compositeObject() {
		XOManager xoManager = getXOManagerFactory().createXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		a.setValue("Value");
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		B b = xoManager.migrate(a, B.class, D.class).as(B.class);
		assertThat(b.getValue(), equalTo("Value"));
		xoManager.currentTransaction().commit();
		xoManager.close();
	}

	@Test
	public void migrationHandler() {
		XOManager xoManager = getXOManagerFactory().createXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		a.setValue("Value");
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		XOManager.MigrationStrategy<A, C> migrationStrategy = new XOManager.MigrationStrategy<A, C>() {
			@Override
			public void migrate(A instance, C target) {
				target.setName(instance.getValue());
			}
		};
		C c = xoManager.migrate(a, migrationStrategy, C.class);
		assertThat(c.getName(), equalTo("Value"));
		xoManager.currentTransaction().commit();
		xoManager.close();
	}

	@Test
	public void compositeObjectMigrationHandler() {
		XOManager xoManager = getXOManagerFactory().createXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		a.setValue("Value");
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		XOManager.MigrationStrategy<A, C> migrationStrategy = new XOManager.MigrationStrategy<A, C>() {
			@Override
			public void migrate(A instance, C target) {
				target.setName(instance.getValue());
			}
		};
		C c = xoManager.migrate(a, migrationStrategy, C.class, D.class).as(
				C.class);
		assertThat(c.getName(), equalTo("Value"));
		xoManager.currentTransaction().commit();
		xoManager.close();
	}
}
