package com.puresoltechnologies.ductiledb.xo.test.data;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@EdgeDefinition("hasBrother")
public interface HasBrother {

	@Outgoing
	Person getSibling();

	void setSibling(Person sibling);

	@Incoming
	Person getBrother();

	void setBrother(Person brother);
}
