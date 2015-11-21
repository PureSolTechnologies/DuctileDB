package com.puresoltechnologies.ductiledb.xo.test.relation.typed;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@EdgeDefinition("OneToOne")
public interface TypedOneToOneRelation extends TypedRelation {

	@Outgoing
	A getA();

	@Incoming
	B getB();

}
