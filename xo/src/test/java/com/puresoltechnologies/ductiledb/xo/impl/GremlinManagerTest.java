package com.puresoltechnologies.ductiledb.xo.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.junit.Test;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.reflection.AnnotatedType;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Query;
import com.puresoltechnologies.ductiledb.xo.api.annotation.QueryLanguage;
import com.puresoltechnologies.ductiledb.xo.impl.query.gremlin.GremlinExpression;
import com.puresoltechnologies.ductiledb.xo.impl.query.gremlin.GremlinManager;

public class GremlinManagerTest {

    @Test
    public void testStringExpression() {
	GremlinExpression expression = GremlinManager.getGremlinExpression("This is a string expression.",
		new HashMap<String, Object>());
	assertThat(expression.getExpression(), is("This is a string expression."));
    }

    @Test
    public void testAnnotatedElementExpression() {
	Query gremlin = mock(Query.class);
	when(gremlin.value()).thenReturn("This is a Gremlin expression.");
	when(gremlin.name()).thenReturn("result");
	when(gremlin.language()).thenReturn(QueryLanguage.GREMLIN);
	AnnotatedType annotatedElement = mock(AnnotatedType.class);
	when(annotatedElement.getAnnotation(Query.class)).thenReturn(gremlin);
	GremlinExpression expression = GremlinManager.getGremlinExpression(annotatedElement,
		new HashMap<String, Object>());
	assertThat(expression.getExpression(), is("This is a Gremlin expression."));
	assertThat(expression.getResultName(), is("result"));
    }

    @Test
    public void testParameterReplacement() {
	HashMap<String, Object> parameters = new HashMap<String, Object>();
	parameters.put("type", 42);
	GremlinExpression expression = GremlinManager.getGremlinExpression("_().has('type', {type})", parameters);
	assertThat(expression.getExpression(), is("type=42\n_().has('type', type)"));
    }

    @Test(expected = XOException.class)
    public void testIllegalQuery() {
	GremlinManager.getGremlinExpression(new Object(), new HashMap<String, Object>());
    }
}
