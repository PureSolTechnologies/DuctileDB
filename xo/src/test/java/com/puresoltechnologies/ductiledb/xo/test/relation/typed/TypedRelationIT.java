package com.puresoltechnologies.ductiledb.xo.test.relation.typed;

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
import com.puresoltechnologies.ductiledb.xo.test.AbstractXOTitanTest;
import com.puresoltechnologies.ductiledb.xo.test.DuctileDBTestUtils;

@RunWith(Parameterized.class)
public class TypedRelationIT extends AbstractXOTitanTest {

	public TypedRelationIT(XOUnit xoUnit) {
		super(xoUnit);
	}

	@Parameterized.Parameters
	public static Collection<XOUnit[]> getXOUnits() throws URISyntaxException {
		return DuctileDBTestUtils.xoUnits(A.class, B.class,
				TypedOneToOneRelation.class, TypedOneToManyRelation.class,
				TypedManyToManyRelation.class);
	}

	@Test
	public void oneToOne() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		B b1 = xoManager.create(B.class);
		B b2 = xoManager.create(B.class);
		TypedOneToOneRelation relation1 = xoManager.create(a,
				TypedOneToOneRelation.class, b1);
		relation1.setVersion(1);
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		assertThat(a.getOneToOne(), equalTo(relation1));
		assertThat(relation1.getVersion(), equalTo(1));
		assertThat(relation1.getA(), equalTo(a));
		assertThat(relation1.getB(), equalTo(b1));
		// assertThat(executeQuery("MATCH ()-[r]->() RETURN r").getColumn("r"),
		// hasItem(equalTo(relation1)));
		TypedOneToOneRelation relation2 = xoManager.create(a,
				TypedOneToOneRelation.class, b2);
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		assertThat(a.getOneToOne(), equalTo(relation2));
		assertThat(relation2.getA(), equalTo(a));
		assertThat(relation2.getB(), equalTo(b2));
		// assertThat(executeQuery("MATCH ()-[r]->() RETURN r").getColumn("r"),
		// hasItem(equalTo(relation2)));
		xoManager.delete(relation2);
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		assertThat(a.getOneToOne(), equalTo(null));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void oneToMany() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		B b1 = xoManager.create(B.class);
		B b2 = xoManager.create(B.class);
		TypedOneToManyRelation relationB1_1 = xoManager.create(a,
				TypedOneToManyRelation.class, b1);
		relationB1_1.setVersion(1);
		TypedOneToManyRelation relationB2_1 = xoManager.create(a,
				TypedOneToManyRelation.class, b2);
		relationB2_1.setVersion(2);
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		assertThat(a.getOneToMany(), hasItems(relationB1_1, relationB2_1));
		assertThat(b1.getManyToOne(), equalTo(relationB1_1));
		assertThat(relationB1_1.getVersion(), equalTo(1));
		assertThat(relationB1_1.getA(), equalTo(a));
		assertThat(relationB1_1.getB(), equalTo(b1));
		assertThat(b2.getManyToOne(), equalTo(relationB2_1));
		assertThat(relationB2_1.getVersion(), equalTo(2));
		assertThat(relationB2_1.getA(), equalTo(a));
		assertThat(relationB2_1.getB(), equalTo(b2));
		// assertThat(executeQuery("MATCH ()-[r]->() RETURN r")
		// .<TypedOneToManyRelation> getColumn("r"),
		// hasItems(relationB1_1, relationB2_1));
		TypedOneToManyRelation relationB1_2 = xoManager.create(a,
				TypedOneToManyRelation.class, b1);
		relationB1_2.setVersion(3);
		TypedOneToManyRelation relationB2_2 = xoManager.create(a,
				TypedOneToManyRelation.class, b2);
		relationB2_2.setVersion(4);
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		assertThat(a.getOneToMany(), hasItems(relationB1_2, relationB2_2));
		assertThat(b1.getManyToOne(), equalTo(relationB1_2));
		assertThat(relationB1_2.getVersion(), equalTo(3));
		assertThat(relationB1_2.getA(), equalTo(a));
		assertThat(relationB1_2.getB(), equalTo(b1));
		assertThat(b2.getManyToOne(), equalTo(relationB2_2));
		assertThat(relationB2_2.getVersion(), equalTo(4));
		assertThat(relationB2_2.getA(), equalTo(a));
		assertThat(relationB2_2.getB(), equalTo(b2));
		// assertThat(executeQuery("MATCH ()-[r]->() RETURN r")
		// .<TypedOneToManyRelation> getColumn("r"),
		// hasItems(relationB1_2, relationB2_2));
		xoManager.delete(relationB1_2);
		xoManager.delete(relationB2_2);
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		// assertThat(executeQuery("MATCH ()-[r]->() RETURN r")
		// .<TypedOneToManyRelation> getColumn("r"), equalTo(null));
		xoManager.currentTransaction().commit();
	}

	@Test
	public void manyToMany() {
		XOManager xoManager = getXOManager();
		xoManager.currentTransaction().begin();
		A a = xoManager.create(A.class);
		B b1 = xoManager.create(B.class);
		B b2 = xoManager.create(B.class);
		TypedManyToManyRelation relationB1_1 = xoManager.create(a,
				TypedManyToManyRelation.class, b1);
		relationB1_1.setVersion(1);
		TypedManyToManyRelation relationB1_2 = xoManager.create(a,
				TypedManyToManyRelation.class, b1);
		relationB1_2.setVersion(2);
		TypedManyToManyRelation relationB2_1 = xoManager.create(a,
				TypedManyToManyRelation.class, b2);
		relationB2_1.setVersion(3);
		TypedManyToManyRelation relationB2_2 = xoManager.create(a,
				TypedManyToManyRelation.class, b2);
		relationB2_2.setVersion(4);
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		assertThat(
				a.getManyToMany(),
				hasItems(relationB1_1, relationB1_2, relationB2_1, relationB2_2));
		assertThat(b1.getManyToMany(), hasItems(relationB1_1, relationB1_2));
		assertThat(b2.getManyToMany(), hasItems(relationB2_1, relationB2_2));
		assertThat(relationB1_1.getVersion(), equalTo(1));
		assertThat(relationB1_1.getA(), equalTo(a));
		assertThat(relationB1_1.getB(), equalTo(b1));
		assertThat(relationB1_2.getVersion(), equalTo(2));
		assertThat(relationB1_2.getA(), equalTo(a));
		assertThat(relationB1_2.getB(), equalTo(b1));
		assertThat(relationB2_1.getVersion(), equalTo(3));
		assertThat(relationB2_1.getA(), equalTo(a));
		assertThat(relationB2_1.getB(), equalTo(b2));
		assertThat(relationB2_2.getVersion(), equalTo(4));
		assertThat(relationB2_2.getA(), equalTo(a));
		assertThat(relationB2_2.getB(), equalTo(b2));
		// assertThat(
		// executeQuery("MATCH ()-[r]->() RETURN r")
		// .<TypedManyToManyRelation> getColumn("r"),
		// hasItems(relationB1_1, relationB1_2, relationB2_1, relationB2_2));
		xoManager.delete(relationB1_1);
		xoManager.delete(relationB2_1);
		xoManager.currentTransaction().commit();
		xoManager.currentTransaction().begin();
		assertThat(b1.getManyToMany(), hasItems(relationB1_2));
		assertThat(b2.getManyToMany(), hasItems(relationB2_2));
		// assertThat(executeQuery("MATCH ()-[r]->() RETURN r")
		// .<TypedManyToManyRelation> getColumn("r"),
		// hasItems(relationB1_2, relationB2_2));
		xoManager.currentTransaction().commit();
	}
}
