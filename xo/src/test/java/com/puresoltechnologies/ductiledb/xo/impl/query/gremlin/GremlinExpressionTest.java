package com.puresoltechnologies.ductiledb.xo.impl.query.gremlin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class GremlinExpressionTest {

    @Test
    public void testCreateFromString() {
	Map<String, Object> parameters = new HashMap<>();
	parameters.put("value", "v1234");
	GremlinExpression expression = GremlinExpression.createGremlinExpression("g.V().has('value',{value})",
		parameters);
	assertNotNull(expression);
	assertEquals("unknown", expression.getResultName());
	assertEquals("g.V().has('value','v1234')", expression.getExpression());
    }

}
