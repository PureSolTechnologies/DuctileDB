package com.puresoltechnologies.ductiledb.xo.test.withSchema;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.buschmais.xo.api.XOManager;
import com.buschmais.xo.api.bootstrap.XOUnit;
import com.puresoltechnologies.ductiledb.api.graph.ElementType;
import com.puresoltechnologies.ductiledb.api.graph.schema.DuctileDBSchemaManager;
import com.puresoltechnologies.ductiledb.api.graph.schema.PropertyDefinition;
import com.puresoltechnologies.ductiledb.api.graph.schema.UniqueConstraint;
import com.puresoltechnologies.ductiledb.xo.test.AbstractXODuctileDBTest;
import com.puresoltechnologies.ductiledb.xo.test.DuctileDBTestUtils;

@RunWith(Parameterized.class)
public class SchemaIT extends AbstractXODuctileDBTest {

    public SchemaIT(XOUnit xoUnit) {
	super(xoUnit);
    }

    @Parameterized.Parameters
    public static Collection<XOUnit[]> getXOUnits() throws URISyntaxException {
	return DuctileDBTestUtils.xoUnits(A.class);
    }

    @Test
    public void validationOnCommitAfterInsert() {
	DuctileDBSchemaManager schemaManager = getGraph().createSchemaManager();
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "neededProperty",
		String.class, UniqueConstraint.TYPE);
	schemaManager.defineProperty(definition);
	Set<String> propertyKeys = new HashSet<>();
	propertyKeys.add(definition.getPropertyKey());
	schemaManager.defineType(ElementType.VERTEX, "A", propertyKeys);

	XOManager xoManager = getXOManager();
	xoManager.currentTransaction().begin();
	xoManager.create(A.class);
    }
}
