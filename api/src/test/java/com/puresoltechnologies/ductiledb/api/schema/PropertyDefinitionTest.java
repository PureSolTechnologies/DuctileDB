package com.puresoltechnologies.ductiledb.api.schema;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.ElementType;

public class PropertyDefinitionTest {

    @Test
    public void testValid() {
	new PropertyDefinition<>(ElementType.VERTEX, "key", String.class, UniqueConstraint.NONE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionForNullElementType() {
	new PropertyDefinition<>(null, "key", String.class, UniqueConstraint.NONE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionForNullPropertyKey() {
	new PropertyDefinition<>(ElementType.VERTEX, null, String.class, UniqueConstraint.NONE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionForEmptyPropertyKey() {
	new PropertyDefinition<>(ElementType.VERTEX, "", String.class, UniqueConstraint.NONE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionForNullPropertyType() {
	new PropertyDefinition<>(ElementType.VERTEX, "key", null, UniqueConstraint.NONE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionForNullUniqueConstraint() {
	new PropertyDefinition<>(ElementType.VERTEX, "key", String.class, null);
    }

}
