package com.puresoltechnologies.ductiledb.storage.engine.memtable;

import java.util.Set;

import com.puresoltechnologies.trees.Tree;

public class RedBlackTree implements Tree<RedBlackTreeNode> {

    private RedBlackTreeNode rootNode;

    @Override
    public RedBlackTreeNode getRootNode() {
	return rootNode;
    }

    @Override
    public Set<RedBlackTreeNode> getVertices() {
	// TODO Auto-generated method stub
	return null;
    }
}
