package com.puresoltechnologies.ductiledb.xo.test.inheritance;

import com.puresoltechnologies.ductiledb.xo.api.annotation.Indexed;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("A")
public interface A extends Version {

	@Indexed
	String getIndex();

	void setIndex(String s);

}
