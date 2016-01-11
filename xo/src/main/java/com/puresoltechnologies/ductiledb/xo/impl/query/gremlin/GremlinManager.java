package com.puresoltechnologies.ductiledb.xo.impl.query.gremlin;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.Gremlin;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.reflection.AnnotatedElement;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Query;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileDBStore;

/**
 * This class manages the Gremlin expressions for the {@link DuctileDBStore}.
 * 
 * @author Rick-Rainer Ludwig
 */
public class GremlinManager {

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
    public static <QL> GremlinExpression getGremlinExpression(QL expression, Map<String, Object> parameters) {
	GremlinExpression gremlinExpression = null;
	if (expression instanceof String) {
	    gremlinExpression = new GremlinExpression("", (String) expression);
	} else if (expression instanceof Query) {
	    Query gremlin = (Query) expression;
	    gremlinExpression = new GremlinExpression(gremlin.name(), gremlin.value());
	} else if (AnnotatedElement.class.isAssignableFrom(expression.getClass())) {
	    AnnotatedElement<?> typeExpression = (AnnotatedElement<?>) expression;
	    gremlinExpression = extractExpression(typeExpression);
	} else if (Class.class.isAssignableFrom(expression.getClass())) {
	    Class<?> clazz = (Class<?>) expression;
	    gremlinExpression = extractExpression(clazz);
	} else if (Method.class.isAssignableFrom(expression.getClass())) {
	    Method method = (Method) expression;
	    gremlinExpression = extractExpression(method);
	} else {
	    throw new XOException(
		    "Unsupported query expression " + expression.toString() + "(class=" + expression.getClass() + ")");
	}
	return applyParameters(parameters, gremlinExpression);
    }

    private static GremlinExpression extractExpression(AnnotatedElement<?> typeExpression) {
	Query gremlin = typeExpression.getAnnotation(Query.class);
	if (gremlin == null) {
	    throw new XOException(typeExpression + " must be annotated with " + Gremlin.class.getName());
	}
	return new GremlinExpression(gremlin);
    }

    private static <QL> GremlinExpression extractExpression(Class<?> clazz) {
	Query gremlin = clazz.getAnnotation(Query.class);
	if (gremlin == null) {
	    throw new XOException(clazz.getName() + " must be annotated with " + Gremlin.class.getName());
	}
	return new GremlinExpression(gremlin);
    }

    private static <QL> GremlinExpression extractExpression(Method method) {
	Query gremlin = method.getAnnotation(Query.class);
	if (gremlin == null) {
	    throw new XOException(method.getName() + " must be annotated with " + Gremlin.class.getName());
	}
	return new GremlinExpression(gremlin);
    }

    private static GremlinExpression applyParameters(Map<String, Object> parameters,
	    GremlinExpression gremlinExpression) {
	StringBuffer typeDefinitions = createTypeDefinitions(parameters);
	String expressionString = gremlinExpression.getExpression();
	for (String type : parameters.keySet()) {
	    String placeholder = "\\{" + type + "\\}";
	    if (!"this".equals(type)) {
		expressionString = expressionString.replaceAll(placeholder, type);
	    } else {
		Object setThis = parameters.get("this");
		if (Vertex.class.isAssignableFrom(setThis.getClass())) {
		    if (!expressionString.startsWith("g.V()")) {
			throw new XOException("Query needs to start with 'g.V()' to be used with start vertex.");
		    }
		    Vertex vertex = (Vertex) setThis;
		    expressionString = expressionString.replace("g.V()", "g.V(" + vertex.id() + ")");
		} else if (Edge.class.isAssignableFrom(setThis.getClass())) {
		    if (!expressionString.startsWith("g.V()")) {
			throw new XOException("Query needs to start with 'g.V()' to be used with start edge.");
		    }
		    Edge edge = (Edge) setThis;
		    expressionString = expressionString.replace("g.V()",
			    "g.V(" + edge.outVertex().id() + "," + edge.inVertex() + ")");
		} else {
		    throw new XOException("Unsupported start point '" + String.valueOf(setThis) + "' (class="
			    + setThis.getClass() + ")");
		}
	    }
	}
	String enhancedExpressionString = typeDefinitions.toString() + expressionString;
	return new GremlinExpression(gremlinExpression.getResultName(), enhancedExpressionString);
    }

    private static StringBuffer createTypeDefinitions(Map<String, Object> parameters) {
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
