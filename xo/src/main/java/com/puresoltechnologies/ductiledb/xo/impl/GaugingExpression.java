package com.puresoltechnologies.ductiledb.xo.impl;

import com.puresoltechnologies.ductiledb.xo.api.annotation.Gauging;

public class GaugingExpression {

	private final String resultName;
	private final String expression;

	public GaugingExpression(String expression) {
		this("", expression);
	}

	public GaugingExpression(String resultName, String expression) {
		super();
		this.resultName = resultName;
		this.expression = expression;
	}

	public GaugingExpression(Gauging gremlin) {
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
