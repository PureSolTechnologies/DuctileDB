package com.puresoltechnologies.ductiledb.core.graph.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.graph.GraphStore;
import com.puresoltechnologies.ductiledb.api.graph.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.core.graph.AbstractDuctileDBGraphTest;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBTestHelper;

public class DuctileDBGraphManagerIT extends AbstractDuctileDBGraphTest {

    private static GraphStore graph;
    private static DuctileDBGraphManager graphManager;

    @BeforeClass
    public static void initialize() {
	graph = getGraph();
	graphManager = graph.createGraphManager();
	assertNotNull("Graph manager was not provided.", graphManager);
    }

    @Test
    public void testVersion() {
	assertEquals("0.2.0", graphManager.getVersion().toString());
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
}
