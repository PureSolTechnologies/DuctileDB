package com.puresoltechnologies.xo.titan.test.delegate;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@EdgeDefinition("RELATION")
public interface A2B {

	@Outgoing
	A getA();

	@Incoming
	B getB();
}
