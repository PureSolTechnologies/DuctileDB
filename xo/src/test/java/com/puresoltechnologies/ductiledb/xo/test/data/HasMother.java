package com.puresoltechnologies.ductiledb.xo.test.data;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@EdgeDefinition("hasMother")
public interface HasMother {

	@Outgoing
	Person getSibling();

	void setSibling(Person sibling);

	@Incoming
	Person getMother();

	void setMother(Person mother);
}
