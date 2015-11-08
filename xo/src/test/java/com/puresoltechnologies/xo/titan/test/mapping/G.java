package com.puresoltechnologies.xo.titan.test.mapping;

import java.util.List;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@VertexDefinition("G")
public interface G {

	@Outgoing
	@EdgeDefinition("ONE_TO_ONE")
	H getOneToOneH();

	void setOneToOneH(H h);

	@Outgoing
	@EdgeDefinition("ONE_TO_MANY")
	List<H> getOneToManyH();

	@Outgoing
	@EdgeDefinition("MANY_TO_MANY")
	List<H> getManyToManyH();

}
