package com.puresoltechnologies.xo.titan.test.query;

import com.puresoltechnologies.ductiledb.xo.api.annotation.Indexed;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("B")
public interface B {

	@Indexed(unique = true)
	String getValue();

	void setValue(String value);

	A2B getA2B();

}
