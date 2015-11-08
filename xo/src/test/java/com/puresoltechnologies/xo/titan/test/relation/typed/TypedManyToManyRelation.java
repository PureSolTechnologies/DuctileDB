package com.puresoltechnologies.xo.titan.test.relation.typed;

import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@EdgeDefinition("ManyToMany")
public interface TypedManyToManyRelation extends TypedRelation {

	@Outgoing
	A getA();

	@Incoming
	B getB();

}
