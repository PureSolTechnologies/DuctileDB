package com.puresoltechnologies.ductiledb.core.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBInvalidPropertyTypeException;
import com.puresoltechnologies.ductiledb.api.DuctileDBPropertyAlreadyDefinedException;
import com.puresoltechnologies.ductiledb.api.ElementType;
import com.puresoltechnologies.ductiledb.api.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.api.manager.PropertyDefinition;
import com.puresoltechnologies.ductiledb.api.manager.UniqueConstraint;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBGraphTest;
import com.puresoltechnologies.ductiledb.core.DuctileDBTestHelper;

public class DuctileDBGraphManagerIT extends AbstractDuctileDBGraphTest {

    private static DuctileDBGraph graph;
    private static DuctileDBGraphManager graphManager;

    @BeforeClass
    public static void initialize() {
	graph = getGraph();
	graphManager = graph.getGraphManager();
    }

    @Before
    public void cleanup() throws IOException {
	DuctileDBTestHelper.removeGraph(graph);
    }

    @Test
    public void testVersion() {
	assertEquals("0.1.0", graphManager.getVersion().toString());
    }

    @Test
    public void testInitialGraphVariables() {
	Iterable<String> variableNames = graphManager.getVariableNames();
	assertNotNull(variableNames);
	assertFalse(variableNames.iterator().hasNext());
    }

    @Test
    public void testSetGetAndRemoveGraphVariable() {
	assertNull(graphManager.getVariable("variable"));
	graphManager.setVariable("variable", "value");
	assertEquals("value", graphManager.getVariable("variable"));
	Iterable<String> variableNames = graphManager.getVariableNames();
	assertNotNull(variableNames);
	Iterator<String> iterator = variableNames.iterator();
	assertTrue(iterator.hasNext());
	assertEquals("variable", iterator.next());
	assertFalse(iterator.hasNext());
	graphManager.removeVariable("variable");
	assertNull(graphManager.getVariable("variable"));
    }

    @Test
    public void testSetGetAndRemoveMultipleGraphVariables() {
	assertNull(graphManager.getVariable("variable1"));
	assertNull(graphManager.getVariable("variable2"));
	graphManager.setVariable("variable1", "value1");
	graphManager.setVariable("variable2", "value2");
	assertEquals("value1", graphManager.getVariable("variable1"));
	assertEquals("value2", graphManager.getVariable("variable2"));
	assertNull(graphManager.getVariable("variable3"));

	Iterable<String> variableNames = graphManager.getVariableNames();
	assertNotNull(variableNames);
	assertEquals(2, DuctileDBTestHelper.count(variableNames));

	graphManager.removeVariable("variable1");
	assertNull(graphManager.getVariable("variable1"));
	assertEquals("value2", graphManager.getVariable("variable2"));
	assertNull(graphManager.getVariable("variable3"));
	graphManager.removeVariable("variable2");
	assertNull(graphManager.getVariable("variable1"));
	assertNull(graphManager.getVariable("variable2"));
	assertNull(graphManager.getVariable("variable3"));
    }

    @Test
    public void testInitialPropertyDefinitions() {
	Iterable<String> definedProperties = graphManager.getDefinedProperties();
	assertNotNull(definedProperties);
	assertFalse(definedProperties.iterator().hasNext());
    }

    @Test
    public void testAddGetAndRemovePropertyDefinitions() {
	assertNull(graphManager.getPropertyDefinition("property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.GLOBAL);
	graphManager.defineProperty(definition);
	PropertyDefinition<String> readDefinition = graphManager.getPropertyDefinition("property");
	assertNotNull(readDefinition);
	assertEquals(definition, readDefinition);
	graphManager.removePropertyDefinition("property");
	assertNull(graphManager.getPropertyDefinition("property"));
    }

    @Test(expected = DuctileDBPropertyAlreadyDefinedException.class)
    public void testDoubleAddedPropertyLeadsToException() {
	assertNull(graphManager.getPropertyDefinition("property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.GLOBAL);
	graphManager.defineProperty(definition);
	PropertyDefinition<String> readDefinition = graphManager.getPropertyDefinition("property");
	assertNotNull(readDefinition);
	assertEquals(definition, readDefinition);
	graphManager.defineProperty(definition);
    }

    @Test
    public void testCorrectPropertyType() {
	assertNull(graphManager.getPropertyDefinition("property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.NONE);
	graphManager.defineProperty(definition);

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
	assertNull(graphManager.getPropertyDefinition("property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.NONE);
	graphManager.defineProperty(definition);

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
}
