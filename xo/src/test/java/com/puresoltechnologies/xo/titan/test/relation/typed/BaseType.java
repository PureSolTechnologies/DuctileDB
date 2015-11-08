package com.puresoltechnologies.xo.titan.test.relation.typed;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

public interface BaseType {

	@Outgoing
	C getC();

	@Incoming
	D getD();

	int getVersion();

	void setVersion(int version);

}
