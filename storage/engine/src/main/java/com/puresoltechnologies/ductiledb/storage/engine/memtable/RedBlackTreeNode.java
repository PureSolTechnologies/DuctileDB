package com.puresoltechnologies.ductiledb.storage.engine.memtable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.puresoltechnologies.trees.TreeLink;
import com.puresoltechnologies.trees.TreeNode;

public class RedBlackTreeNode implements TreeNode<RedBlackTreeNode> {

    private final RedBlackTreeNode parent;
    private RedBlackTreeNode left = null;
    private RedBlackTreeNode right = null;

    public RedBlackTreeNode(RedBlackTreeNode parent) {
	super();
	this.parent = parent;
    }

    @Override
    public RedBlackTreeNode getParent() {
	return parent;
    }

    @Override
    public Set<TreeLink<RedBlackTreeNode>> getEdges() {
	Set<TreeLink<RedBlackTreeNode>> edges = new HashSet<>();
	edges.add(new TreeLink<>(this, left));
	edges.add(new TreeLink<>(this, right));
	edges.add(new TreeLink<>(parent, this));
	return edges;
    }

    @Override
    public boolean hasChildren() {
	return (left != null) || (right != null);
    }

    @Override
    public List<RedBlackTreeNode> getChildren() {
	List<RedBlackTreeNode> children = new ArrayList<>();
	children.add(left);
	children.add(right);
	return children;
    }

    @Override
    public String getName() {
	// TODO Auto-generated method stub
	return null;
    }

}
