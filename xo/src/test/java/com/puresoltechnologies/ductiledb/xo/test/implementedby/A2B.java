package com.puresoltechnologies.ductiledb.xo.test.implementedby;

import com.buschmais.xo.api.annotation.ImplementedBy;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@EdgeDefinition("A2B")
public interface A2B {

	@Outgoing
	A getA();

	@Incoming
	B getB();

	int getValue();

	void setValue(int i);

	@ImplementedBy(SetMethod.class)
	void setCustomValue(String usingHandler);

	@ImplementedBy(GetMethod.class)
	String getCustomValue();

	@ImplementedBy(RelationIncrementValueMethod.class)
	int incrementValue();

	void unsupportedOperation();
}
