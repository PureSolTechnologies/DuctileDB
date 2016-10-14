package com.puresoltechnologies.ductiledb.core.graph.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.graph.AbstractDuctileDBGraphTest;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.graph.ElementType;
import com.puresoltechnologies.ductiledb.core.graph.GraphStore;

public class DuctileDBSchemaManagerIT extends AbstractDuctileDBGraphTest {

    private static GraphStore graph;
    private static DuctileDBSchemaManager schemaManager;

    @BeforeClass
    public static void initialize() {
	graph = getGraph();
	schemaManager = graph.createSchemaManager();
	assertNotNull("Schema manager was not provided.", schemaManager);
    }

    @Test
    public void testInitialPropertyDefinitions() {
	Iterable<String> definedProperties = schemaManager.getDefinedProperties();
	assertNotNull(definedProperties);
	assertFalse(definedProperties.iterator().hasNext());
    }

    @Test
    public void testAddGetAndRemovePropertyDefinitions() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(definition);
	PropertyDefinition<String> readDefinition = schemaManager.getPropertyDefinition(ElementType.VERTEX, "property");
	assertNotNull(readDefinition);
	assertEquals(definition, readDefinition);
	schemaManager.removePropertyDefinition(ElementType.VERTEX, "property");
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDefinePropertyWithNullThrowsIllegalArgumentException() {
	schemaManager.defineProperty(null);
    }

    @Test(expected = DuctileDBPropertyAlreadyDefinedException.class)
    public void testDoubleAddedPropertyLeadsToException() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(definition);
	PropertyDefinition<String> readDefinition = schemaManager.getPropertyDefinition(ElementType.VERTEX, "property");
	assertNotNull(readDefinition);
	assertEquals(definition, readDefinition);
	schemaManager.defineProperty(definition);
    }

    @Test
    public void testCorrectPropertyType() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.NONE);
	schemaManager.defineProperty(definition);

	Set<String> types = new HashSet<>();
	types.add("type");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "StringValue");
	try {
	    graph.addVertex(types, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBInvalidPropertyTypeException.class)
    public void testIncorrectPropertyType() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.NONE);
	schemaManager.defineProperty(definition);

	Set<String> types = new HashSet<>();
	types.add("type");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property", 1l);
	try {
	    graph.addVertex(types, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBInvalidPropertyTypeException.class)
    public void testIncorrectPropertyType2() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.NONE);
	schemaManager.defineProperty(definition);

	Set<String> types = new HashSet<>();
	types.add("type");
	try {
	    DuctileDBVertex vertex = graph.addVertex(types, new HashMap<>());
	    vertex.setProperty("property", 1l);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBUniqueConstraintViolationException.class)
    public void testGlobalVertexUniqueConstraint() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(definition);

	Set<String> types1 = new HashSet<>();
	types1.add("type1");
	Set<String> types2 = new HashSet<>();
	types2.add("type2");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    graph.addVertex(types1, properties);
	    graph.addVertex(types2, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test
    public void testVertexTypeUniqueConstraintValid() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.TYPE);
	schemaManager.defineProperty(definition);

	Set<String> types1 = new HashSet<>();
	types1.add("type1");
	Set<String> types2 = new HashSet<>();
	types2.add("type2");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    graph.addVertex(types1, properties);
	    graph.addVertex(types2, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBUniqueConstraintViolationException.class)
    public void testVertexTypeUniqueConstraintViolation() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.TYPE);
	schemaManager.defineProperty(definition);

	Set<String> types1 = new HashSet<>();
	types1.add("type1");
	Set<String> types2 = new HashSet<>();
	types2.add("type1");
	types2.add("type2");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    graph.addVertex(types1, properties);
	    graph.addVertex(types2, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBUniqueConstraintViolationException.class)
    public void testGlobalVertexUniqueConstraintSetProperty() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(definition);

	Set<String> types1 = new HashSet<>();
	types1.add("type1");
	Set<String> types2 = new HashSet<>();
	types2.add("type2");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    graph.addVertex(types1, properties);
	    graph.addVertex().setProperty("property", "value");
	} finally {
	    graph.rollback();
	}
    }

    @Test
    public void testVertexTypeUniqueConstraintValidSetProperty() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.TYPE);
	schemaManager.defineProperty(definition);

	Set<String> types1 = new HashSet<>();
	types1.add("type1");
	Set<String> types2 = new HashSet<>();
	types2.add("type2");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    graph.addVertex(types1, properties);
	    graph.addVertex().setProperty("property", "value");
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBUniqueConstraintViolationException.class)
    public void testVertexTypeUniqueConstraintViolationSetProperty() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.TYPE);
	schemaManager.defineProperty(definition);

	Set<String> types1 = new HashSet<>();
	types1.add("type1");
	Set<String> types2 = new HashSet<>();
	types2.add("type1");
	types2.add("type2");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    graph.addVertex(types1, properties);
	    graph.addVertex(types2, new HashMap<>()).setProperty("property", "value");
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBUniqueConstraintViolationException.class)
    public void testGlobalEdgeUniqueConstraint() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.EDGE, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.EDGE, "property", String.class,
		UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(definition);

	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    DuctileDBVertex vertex1 = graph.addVertex();
	    DuctileDBVertex vertex2 = graph.addVertex();
	    DuctileDBVertex vertex3 = graph.addVertex();
	    vertex1.addEdge("type1", vertex2, properties);
	    vertex2.addEdge("type2", vertex3, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test
    public void testEdgeTypeUniqueConstraintValid() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.EDGE, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.EDGE, "property", String.class,
		UniqueConstraint.TYPE);
	schemaManager.defineProperty(definition);

	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    DuctileDBVertex vertex1 = graph.addVertex();
	    DuctileDBVertex vertex2 = graph.addVertex();
	    DuctileDBVertex vertex3 = graph.addVertex();
	    vertex1.addEdge("type1", vertex2, properties);
	    vertex2.addEdge("type2", vertex3, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBUniqueConstraintViolationException.class)
    public void testEdgeTypeUniqueConstraintViolation() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.EDGE, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.EDGE, "property", String.class,
		UniqueConstraint.TYPE);
	schemaManager.defineProperty(definition);

	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    DuctileDBVertex vertex1 = graph.addVertex();
	    DuctileDBVertex vertex2 = graph.addVertex();
	    DuctileDBVertex vertex3 = graph.addVertex();
	    vertex1.addEdge("type1", vertex2, properties);
	    vertex2.addEdge("type1", vertex3, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBUniqueConstraintViolationException.class)
    public void testGlobalEdgeUniqueConstraintSetProperty() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.EDGE, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.EDGE, "property", String.class,
		UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(definition);

	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    DuctileDBVertex vertex1 = graph.addVertex();
	    DuctileDBVertex vertex2 = graph.addVertex();
	    DuctileDBVertex vertex3 = graph.addVertex();
	    vertex1.addEdge("type1", vertex2, properties);
	    vertex2.addEdge("type2", vertex3, new HashMap<>()).setProperty("property", "value");
	} finally {
	    graph.rollback();
	}
    }

    @Test
    public void testEdgeTypeUniqueConstraintValidSetProperty() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.EDGE, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.EDGE, "property", String.class,
		UniqueConstraint.TYPE);
	schemaManager.defineProperty(definition);

	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    DuctileDBVertex vertex1 = graph.addVertex();
	    DuctileDBVertex vertex2 = graph.addVertex();
	    DuctileDBVertex vertex3 = graph.addVertex();
	    vertex1.addEdge("type1", vertex2, properties);
	    vertex2.addEdge("type2", vertex3, new HashMap<>()).setProperty("property", "value");
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBUniqueConstraintViolationException.class)
    public void testEdgeTypeUniqueConstraintViolationSetProperty() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.EDGE, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.EDGE, "property", String.class,
		UniqueConstraint.TYPE);
	schemaManager.defineProperty(definition);

	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    DuctileDBVertex vertex1 = graph.addVertex();
	    DuctileDBVertex vertex2 = graph.addVertex();
	    DuctileDBVertex vertex3 = graph.addVertex();
	    vertex1.addEdge("type1", vertex2, properties);
	    vertex2.addEdge("type1", vertex3, new HashMap<>()).setProperty("property", "value");
	} finally {
	    graph.rollback();
	}
    }

    @Test
    public void testInitialTypeDefinitions() {
	Iterable<String> definedTypes = schemaManager.getDefinedTypes();
	assertNotNull(definedTypes);
	assertFalse(definedTypes.iterator().hasNext());
    }

    @Test
    public void testAddGetAndRemoveTypeDefinitions() {
	PropertyDefinition<String> propertyDefinition = new PropertyDefinition<>(ElementType.VERTEX, "property",
		String.class, UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(propertyDefinition);
	assertNull(schemaManager.getTypeDefinition(ElementType.VERTEX, "type"));
	Set<String> propertyKeys = new HashSet<>();
	propertyKeys.add("property");
	schemaManager.defineType(ElementType.VERTEX, "type", propertyKeys);
	Set<String> readPropertyKeys = schemaManager.getTypeDefinition(ElementType.VERTEX, "type");
	assertNotNull(readPropertyKeys);
	assertEquals(propertyKeys, readPropertyKeys);
	schemaManager.removeTypeDefinition(ElementType.VERTEX, "type");
	assertNull(schemaManager.getTypeDefinition(ElementType.VERTEX, "type"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDefineTypeThrowsIllegalArgumentExceptionForNullElementType() {
	PropertyDefinition<String> propertyDefinition = new PropertyDefinition<>(ElementType.VERTEX, "property",
		String.class, UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(propertyDefinition);
	Set<String> propertyKeys = new HashSet<>();
	propertyKeys.add("property");
	schemaManager.defineType(null, "type", propertyKeys);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDefineTypeThrowsIllegalArgumentExceptionForNullTypeName() {
	PropertyDefinition<String> propertyDefinition = new PropertyDefinition<>(ElementType.VERTEX, "property",
		String.class, UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(propertyDefinition);
	Set<String> propertyKeys = new HashSet<>();
	propertyKeys.add("property");
	schemaManager.defineType(ElementType.VERTEX, null, propertyKeys);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDefineTypeThrowsIllegalArgumentExceptionForEmtpyTypeName() {
	PropertyDefinition<String> propertyDefinition = new PropertyDefinition<>(ElementType.VERTEX, "property",
		String.class, UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(propertyDefinition);
	Set<String> propertyKeys = new HashSet<>();
	propertyKeys.add("property");
	schemaManager.defineType(ElementType.VERTEX, "", propertyKeys);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDefineTypeThrowsIllegalArgumentExceptionForNullPropertyKeys() {
	schemaManager.defineType(ElementType.VERTEX, "type", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDefineTypeThrowsIllegalArgumentExceptionForEmtpyPropertyKeys() {
	schemaManager.defineType(ElementType.VERTEX, "type", new HashSet<>());
    }

    @Test(expected = DuctileDBSchemaException.class)
    public void testDefineTypeThrowsDuctileDBSchemaExceptionForUnknownPropertyKey() {
	Set<String> propertyKeys = new HashSet<>();
	propertyKeys.add("unknownProperty");
	schemaManager.defineType(ElementType.VERTEX, "type", propertyKeys);
    }

    @Test
    public void testNormalCreationWithTypeSchema() {
	schemaManager.defineProperty(
		new PropertyDefinition<>(ElementType.VERTEX, "property1", String.class, UniqueConstraint.NONE));
	schemaManager.defineProperty(
		new PropertyDefinition<>(ElementType.VERTEX, "property2", Long.class, UniqueConstraint.NONE));
	Set<String> propertyKeys = new HashSet<>();
	propertyKeys.add("property1");
	propertyKeys.add("property2");
	schemaManager.defineType(ElementType.VERTEX, "type", propertyKeys);

	Set<String> types = new HashSet<>();
	types.add("type");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property1", "stringValue");
	properties.put("property2", 1234567890l);
	graph.addVertex(types, properties);
	graph.commit();
    }

    @Test(expected = DuctileDBSchemaException.class)
    public void testCreationWithTypeSchemaWithMissingProperty() {
	schemaManager.defineProperty(
		new PropertyDefinition<>(ElementType.VERTEX, "property1", String.class, UniqueConstraint.NONE));
	schemaManager.defineProperty(
		new PropertyDefinition<>(ElementType.VERTEX, "property2", Long.class, UniqueConstraint.NONE));
	Set<String> propertyKeys = new HashSet<>();
	propertyKeys.add("property1");
	propertyKeys.add("property2");
	schemaManager.defineType(ElementType.VERTEX, "type", propertyKeys);

	Set<String> types = new HashSet<>();
	types.add("type");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property1", "stringValue");
	graph.addVertex(types, properties);
	graph.commit();
    }

    @Test(expected = DuctileDBSchemaException.class)
    public void testDeletionOfPropertyNeededForSchema() {
	schemaManager.defineProperty(
		new PropertyDefinition<>(ElementType.VERTEX, "property1", String.class, UniqueConstraint.NONE));
	schemaManager.defineProperty(
		new PropertyDefinition<>(ElementType.VERTEX, "property2", Long.class, UniqueConstraint.NONE));
	Set<String> propertyKeys = new HashSet<>();
	propertyKeys.add("property1");
	propertyKeys.add("property2");
	schemaManager.defineType(ElementType.VERTEX, "type", propertyKeys);

	Set<String> types = new HashSet<>();
	types.add("type");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property1", "stringValue");
	properties.put("property2", 1234567890l);
	DuctileDBVertex vertex = graph.addVertex(types, properties);
	graph.commit();

	vertex.removeProperty("property1");
    }

}
