package com.puresoltechnologies.ductiledb.xo.test.query;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@EdgeDefinition("A2B")
public interface A2B {

	@Outgoing
	A getA();

	@Incoming
	B getB();

}
