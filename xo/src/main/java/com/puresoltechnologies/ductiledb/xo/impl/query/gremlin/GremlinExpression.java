package com.puresoltechnologies.ductiledb.xo.impl.query.gremlin;

import java.lang.reflect.Method;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.Gremlin;

import com.buschmais.xo.api.XOException;
import com.buschmais.xo.spi.reflection.AnnotatedElement;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Query;

public class GremlinExpression {

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
    public static <QL> GremlinExpression createGremlinExpression(QL expression, Map<String, Object> parameters) {
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
	gremlinExpression.applyParameters(parameters);
	return gremlinExpression;
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

    private final String resultName;
    private String expression;

    public GremlinExpression(String expression) {
	this("", expression);
    }

    public GremlinExpression(String resultName, String expression) {
	super();
	this.resultName = resultName;
	this.expression = expression;
    }

    public GremlinExpression(Query gremlin) {
	this(gremlin.name(), gremlin.value());
    }

    public String getResultName() {
	return (resultName == null) || (resultName.isEmpty()) ? "unknown" : resultName;
    }

    public String getExpression() {
	return expression;
    }

    public void applyParameters(Map<String, Object> parameters) {
	String expressionString = getExpression();
	for (String parameter : parameters.keySet()) {
	    String placeholder = "\\{" + parameter + "\\}";
	    if (!"this".equals(parameter)) {
		Object value = parameters.get(parameter);
		if (String.class.equals(value.getClass())) {
		    expressionString = expressionString.replaceAll(placeholder, "'" + value + "'");
		} else {
		    expressionString = expressionString.replaceAll(placeholder, value.toString());
		}
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
	expression = expressionString;
    }

    @Override
    public String toString() {
	return resultName + ":=" + expression;
    }
}
