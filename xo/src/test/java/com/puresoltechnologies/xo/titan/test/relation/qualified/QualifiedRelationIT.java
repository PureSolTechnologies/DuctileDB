package com.puresoltechnologies.xo.titan.test.relation.qualified;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
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
public class QualifiedRelationIT extends AbstractXOTitanTest {

    public QualifiedRelationIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getXOUnits() throws URISyntaxException {
	return XOTitanTestUtils.xoUnits(A.class, B.class);
    }

    @Test
    public void oneToOne() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a = xoManager.create(A.class);
	B b1 = xoManager.create(B.class);
	a.setOneToOne(b1);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	assertThat(a.getOneToOne(), equalTo(b1));
	assertThat(b1.getOneToOne(), equalTo(a));
	// assertThat(executeQuery("MATCH (a:A)-[:OneToOne]->(b:B) RETURN b")
	// .getColumn("b"), hasItem(b1));
	B b2 = xoManager.create(B.class);
	a.setOneToOne(b2);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	assertThat(a.getOneToOne(), equalTo(b2));
	assertThat(b2.getOneToOne(), equalTo(a));
	assertThat(b1.getOneToOne(), equalTo(null));
	// assertThat(executeQuery("MATCH (a:A)-[:OneToOne]->(b:B) RETURN b")
	// .getColumn("b"), hasItem(b2));
	a.setOneToOne(null);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	assertThat(a.getOneToOne(), equalTo(null));
	assertThat(b1.getOneToOne(), equalTo(null));
	assertThat(b2.getOneToOne(), equalTo(null));
	xoManager.currentTransaction().commit();
    }

    @Test
    public void oneToMany() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a = xoManager.create(A.class);
	B b1 = xoManager.create(B.class);
	B b2 = xoManager.create(B.class);
	a.getOneToMany().add(b1);
	a.getOneToMany().add(b2);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	assertThat(a.getOneToMany(), hasItems(b1, b2));
	assertThat(b1.getManyToOne(), equalTo(a));
	assertThat(b2.getManyToOne(), equalTo(a));
	// assertThat(executeQuery("MATCH (a:A)-[:OneToMany]->(b:B) RETURN b")
	// .<B> getColumn("b"), hasItems(b1, b2));
	a.getOneToMany().remove(b1);
	a.getOneToMany().remove(b2);
	B b3 = xoManager.create(B.class);
	B b4 = xoManager.create(B.class);
	a.getOneToMany().add(b3);
	a.getOneToMany().add(b4);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	assertThat(a.getOneToMany(), hasItems(b3, b4));
	assertThat(b1.getManyToOne(), equalTo(null));
	assertThat(b2.getManyToOne(), equalTo(null));
	assertThat(b3.getManyToOne(), equalTo(a));
	assertThat(b4.getManyToOne(), equalTo(a));
	// assertThat(executeQuery("MATCH (a:A)-[:OneToMany]->(b:B) RETURN b")
	// .<B> getColumn("b"), hasItems(b3, b4));
	xoManager.currentTransaction().commit();
    }

    @Test
    public void manyToMany() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a1 = xoManager.create(A.class);
	A a2 = xoManager.create(A.class);
	B b1 = xoManager.create(B.class);
	B b2 = xoManager.create(B.class);
	a1.getManyToMany().add(b1);
	a1.getManyToMany().add(b2);
	a2.getManyToMany().add(b1);
	a2.getManyToMany().add(b2);
	xoManager.currentTransaction().commit();

	xoManager.currentTransaction().begin();
	assertThat(a1.getManyToMany(), hasItems(b1, b2));
	assertThat(a2.getManyToMany(), hasItems(b1, b2));
	assertThat(b1.getManyToMany(), hasItems(a1, a2));
	assertThat(b2.getManyToMany(), hasItems(a1, a2));
	// assertThat(
	// executeQuery(
	// "MATCH (a:A)-[:ManyToMany]->(b:B) RETURN a, collect(b) as listOfB ORDER BY ID(a)")
	// .<A> getColumn("a"), hasItems(a1, a2));
	// assertThat(
	// executeQuery(
	// "MATCH (a:A)-[:ManyToMany]->(b:B) RETURN a, collect(b) as listOfB ORDER BY ID(a)")
	// .<Iterable<B>> getColumn("listOfB"),
	// hasItems(hasItems(b1, b2), hasItems(b1, b2)));
	a1.getManyToMany().remove(b1);
	a2.getManyToMany().remove(b1);
	xoManager.currentTransaction().commit();
	xoManager.currentTransaction().begin();
	assertThat(a1.getManyToMany(), hasItems(b2));
	assertThat(a2.getManyToMany(), hasItems(b2));
	assertThat(b1.getManyToMany().isEmpty(), equalTo(true));
	assertThat(b2.getManyToMany(), hasItems(a1, a2));
	xoManager.currentTransaction().commit();
    }

    @Test
    public void test() {
	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	A a1 = xoManager.create(A.class);
	A a2 = xoManager.create(A.class);
	B b1 = xoManager.create(B.class);
	B b2 = xoManager.create(B.class);
	a1.getManyToMany().add(b1);
	a1.getManyToMany().add(b2);
	xoManager.currentTransaction().commit();
	xoManager.currentTransaction().begin();
	for (B b : a1.getOneToMany()) {
	    System.out.println(b.getClass().getName());
	}
	for (B b : a2.getOneToMany()) {
	    System.out.println(b.getClass().getName());
	}
	xoManager.currentTransaction().rollback();
    }
}
