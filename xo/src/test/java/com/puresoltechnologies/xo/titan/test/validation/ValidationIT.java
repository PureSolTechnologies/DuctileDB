package com.puresoltechnologies.xo.titan.test.validation;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.annotation.PreUpdate;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.xo.titan.test.AbstractXOTitanTest;
import com.puresoltechnologies.xo.titan.test.XOTitanTestUtils;

@RunWith(Parameterized.class)
public class ValidationIT extends AbstractXOTitanTest {

	public ValidationIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getXOUnits() throws URISyntaxException {
		return XOTitanTestUtils.xoUnits(A.class, B.class);
	}

	@Test
	public void validationOnCommitAfterInsert() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		Set<ConstraintViolation<?>> constraintViolations = null;
		try {
			xoManager.currentTransaction().commit();
			Assert.fail("Validation must fail.");
		} catch (ConstraintViolationException e) {
			constraintViolations = e.getConstraintViolations();
		}
		assertThat(constraintViolations.size(), equalTo(2));
		B b = xoManager.create(B.class);
		a.setB(b);
		a.setName("Indiana Jones");
		xoManager.currentTransaction().commit();
	}

	@Test
	public void validationOnCommitAfterQuery() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		B b = xoManager.create(B.class);
		for (int i = 0; i < 2; i++) {
			A a = xoManager.create(A.class);
			a.setName("Miller");
			a.setB(b);
		}
		xoManager.currentTransaction().commit();
		closeXOManager();
		xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		for (A miller : xoManager.find(A.class, "Miller")) {
			miller.setName(null);
		}
		Set<ConstraintViolation<?>> constraintViolations = null;
		try {
			xoManager.currentTransaction().commit();
			Assert.fail("Validation must fail.");
		} catch (ConstraintViolationException e) {
			constraintViolations = e.getConstraintViolations();
		}
		assertThat(constraintViolations.size(), equalTo(1));
		xoManager.currentTransaction().rollback();
	}

	@Test
	public void validationAfterPreUpdate() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		B b = xoManager.create(B.class);
		A a = xoManager.create(A.class);
		a.setB(b);
		Set<ConstraintViolation<?>> constraintViolations = null;
		try {
			xoManager.currentTransaction().commit();
		} catch (ConstraintViolationException e) {
			constraintViolations = e.getConstraintViolations();
		}
		assertThat(constraintViolations.size(), equalTo(1));
		xoManager.registerInstanceListener(new InstanceListener());
		xoManager.currentTransaction().commit();
	}

	public static final class InstanceListener {
		@PreUpdate
		public void setName(A instance) {
			instance.setName("Miller");
		}
	}
}
