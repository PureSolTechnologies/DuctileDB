package com.puresoltechnologies.ductiledb.xo.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.junit.Test;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.reflection.AnnotatedType;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Gauging;
import com.puresoltechnologies.ductiledb.xo.impl.GaugingExpression;
import com.puresoltechnologies.ductiledb.xo.impl.GaugingManager;

public class GaugingManagerTest {

    @Test
    public void testStringExpression() {
	GaugingExpression expression = GaugingManager.getGaugingExpression("This is a string expression.",
		new HashMap<String, Object>());
	assertThat(expression.getExpression(), is("This is a string expression."));
    }

    @Test
    public void testAnnotatedElementExpression() {
	Gauging gauging = mock(Gauging.class);
	when(gauging.value()).thenReturn("This is a Gauging expression.");
	when(gauging.name()).thenReturn("result");
	AnnotatedType annotatedElement = mock(AnnotatedType.class);
	when(annotatedElement.getAnnotation(Gauging.class)).thenReturn(gauging);
	GaugingExpression expression = GaugingManager.getGaugingExpression(annotatedElement,
		new HashMap<String, Object>());
	assertThat(expression.getExpression(), is("This is a Gauging expression."));
	assertThat(expression.getResultName(), is("result"));
    }

    @Test
    public void testParameterReplacement() {
	HashMap<String, Object> parameters = new HashMap<String, Object>();
	parameters.put("type", 42);
	GaugingExpression expression = GaugingManager.getGaugingExpression("_().has('type', {type})", parameters);
	assertThat(expression.getExpression(), is("type=42\n_().has('type', type)"));
    }

    @Test(expected = XOException.class)
    public void testIllegalQuery() {
	GaugingManager.getGaugingExpression(new Object(), new HashMap<String, Object>());
    }
}
