package com.puresoltechnologies.ductiledb.xo.test.mapping;

import java.util.List;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;

@VertexDefinition("H")
public interface H {

	@EdgeDefinition("ONE_TO_ONE")
	@Incoming
	G getOneToOneG();

	void setOneToOneG(G g);

	@Incoming
	@EdgeDefinition("ONE_TO_MANY")
	G getManyToOneG();

	void setManyToOneG(G g);

	@Incoming
	@EdgeDefinition("MANY_TO_MANY")
	List<G> getManyToManyG();
}
