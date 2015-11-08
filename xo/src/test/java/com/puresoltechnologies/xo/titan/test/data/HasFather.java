package com.puresoltechnologies.xo.titan.test.data;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@EdgeDefinition("hasFather")
public interface HasFather {

	@Outgoing
	Person getSibling();

	void setSibling(Person sibling);

	@Incoming
	Person getFather();

	void setFather(Person father);

}
