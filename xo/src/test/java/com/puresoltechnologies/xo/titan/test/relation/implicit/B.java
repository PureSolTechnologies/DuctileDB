package com.puresoltechnologies.xo.titan.test.relation.implicit;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;

@VertexDefinition
public interface B {

	@Incoming
	@ImplicitOneToOne
	A getOneToOne();

	void setOneToOne(A a);
}
