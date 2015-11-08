package com.puresoltechnologies.xo.titan.test.relation.typed;

import java.util.List;

import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;

@VertexDefinition
public interface TreeNode {

	void setName(String name);

	String getName();

	@Incoming
	TreeNodeRelation getParent();

	@Outgoing
	List<TreeNodeRelation> getChildren();

}
