package com.puresoltechnologies.ductiledb.xo.impl;

import com.puresoltechnologies.ductiledb.xo.api.annotation.Gauging;

public class GremlinExpression {

	private final String resultName;
	private final String expression;

	public GremlinExpression(String expression) {
		this("", expression);
	}

	public GremlinExpression(String resultName, String expression) {
		super();
		this.resultName = resultName;
		this.expression = expression;
	}

	public GremlinExpression(Gauging gremlin) {
		this(gremlin.name(), gremlin.value());
	}

	public String getResultName() {
		return (resultName == null) || (resultName.isEmpty()) ? "unknown"
				: resultName;
	}

	public String getExpression() {
		return expression;
	}

	@Override
	public String toString() {
		return resultName + ":=" + expression;
	}
}
