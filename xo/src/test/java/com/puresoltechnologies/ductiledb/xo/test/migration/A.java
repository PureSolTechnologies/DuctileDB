package com.puresoltechnologies.ductiledb.xo.test.migration;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("A")
public interface A {

	String getValue();

	void setValue(String value);

}
