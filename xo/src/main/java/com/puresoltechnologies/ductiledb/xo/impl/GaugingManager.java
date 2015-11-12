package com.puresoltechnologies.ductiledb.xo.impl;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Map.Entry;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.reflection.AnnotatedElement;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Gauging;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

/**
 * This class manages the Gremlin expressions for the
 * {@link DuctileDBStore}.
 * 
 * @author Rick-Rainer Ludwig
 */
public class GaugingManager {

    /**
     * This is a helper method to extract the Gremlin expression.
     * 
     * @param expression
     *            is the object which comes in from
     *            DatastoreSession#executeQuery(Object, java.util.Map).
     * @param parameters
     *            is a Map of parameters.
     * @return A {@link String} containing a Gremlin expression is returned.
     * @param <QL>
     *            is the query language.
     */
    public static <QL> GaugingExpression getGaugingExpression(QL expression,
	    Map<String, Object> parameters) {
	GaugingExpression gremlinExpression = null;
	if (expression instanceof String) {
	    gremlinExpression = new GaugingExpression("", (String) expression);
	} else if (expression instanceof Gauging) {
	    Gauging gremlin = (Gauging) expression;
	    gremlinExpression = new GaugingExpression(gremlin.name(),
		    gremlin.value());
	} else if (AnnotatedElement.class.isAssignableFrom(expression
		.getClass())) {
	    AnnotatedElement<?> typeExpression = (AnnotatedElement<?>) expression;
	    gremlinExpression = extractExpression(typeExpression);
	} else if (Class.class.isAssignableFrom(expression.getClass())) {
	    Class<?> clazz = (Class<?>) expression;
	    gremlinExpression = extractExpression(clazz);
	} else if (Method.class.isAssignableFrom(expression.getClass())) {
	    Method method = (Method) expression;
	    gremlinExpression = extractExpression(method);
	} else {
	    throw new XOException("Unsupported query expression "
		    + expression.toString() + "(class=" + expression.getClass()
		    + ")");
	}
	return applyParameters(parameters, gremlinExpression);
    }

    private static GaugingExpression extractExpression(
	    AnnotatedElement<?> typeExpression) {
	Gauging gremlin = typeExpression.getAnnotation(Gauging.class);
	if (gremlin == null) {
	    throw new XOException(typeExpression + " must be annotated with "
		    + Gauging.class.getName());
	}
	return new GaugingExpression(gremlin);
    }

    private static <QL> GaugingExpression extractExpression(Class<?> clazz) {
	Gauging gremlin = clazz.getAnnotation(Gauging.class);
	if (gremlin == null) {
	    throw new XOException(clazz.getName() + " must be annotated with "
		    + Gauging.class.getName());
	}
	return new GaugingExpression(gremlin);
    }

    private static <QL> GaugingExpression extractExpression(Method method) {
	Gauging gremlin = method.getAnnotation(Gauging.class);
	if (gremlin == null) {
	    throw new XOException(method.getName() + " must be annotated with "
		    + Gauging.class.getName());
	}
	return new GaugingExpression(gremlin);
    }

    private static GaugingExpression applyParameters(
	    Map<String, Object> parameters, GaugingExpression gremlinExpression) {
	StringBuffer typeDefinitions = createTypeDefinitions(parameters);
	String expressionString = gremlinExpression.getExpression();
	for (String type : parameters.keySet()) {
	    String placeholder = "\\{" + type + "\\}";
	    if (!"this".equals(type)) {
		expressionString = expressionString.replaceAll(placeholder,
			type);
	    }
	}
	String enhancedExpressionString = typeDefinitions.toString()
		+ expressionString;
	return new GaugingExpression(gremlinExpression.getResultName(),
		enhancedExpressionString);
    }

    private static StringBuffer createTypeDefinitions(
	    Map<String, Object> parameters) {
	StringBuffer typeDefinitions = new StringBuffer();
	for (Entry<String, Object> entry : parameters.entrySet()) {
	    String type = entry.getKey();
	    if (!"this".equals(type)) {
		Object value = entry.getValue();
		if (String.class.equals(value.getClass())) {
		    typeDefinitions.append(type);
		    typeDefinitions.append("=");
		    typeDefinitions.append("'");
		    typeDefinitions.append(value);
		    typeDefinitions.append("'");
		    typeDefinitions.append("\n");
		} else if (Edge.class.isAssignableFrom(value.getClass())) {
		    continue;
		} else if (Vertex.class.isAssignableFrom(value.getClass())) {
		    continue;
		} else {
		    typeDefinitions.append(type);
		    typeDefinitions.append("=");
		    typeDefinitions.append(value);
		    typeDefinitions.append("\n");
		}
	    }
	}
	return typeDefinitions;
    }

}
