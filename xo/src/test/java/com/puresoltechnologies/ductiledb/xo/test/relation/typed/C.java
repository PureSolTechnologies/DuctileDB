package com.puresoltechnologies.ductiledb.xo.test.relation.typed;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition("C")
public interface C {

	TypeA getTypeA();

	TypeB getTypeB();

}
