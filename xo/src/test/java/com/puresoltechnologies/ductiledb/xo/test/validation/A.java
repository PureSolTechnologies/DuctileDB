package com.puresoltechnologies.ductiledb.xo.test.validation;

import javax.validation.constraints.NotNull;

import com.puresoltechnologies.ductiledb.xo.api.annotation.Indexed;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("A")
public interface A {

	@NotNull
	@Indexed
	String getName();

	void setName(String name);

	@NotNull
	B getB();

	void setB(B b);
}
