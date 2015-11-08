package com.puresoltechnologies.xo.titan.test.relation.implicit;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@VertexDefinition
public interface A {

	@Outgoing
	@ImplicitOneToOne
	B getOneToOne();

	void setOneToOne(B b);

}
