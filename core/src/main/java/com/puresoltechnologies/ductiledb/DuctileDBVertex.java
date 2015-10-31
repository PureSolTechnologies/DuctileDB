package com.puresoltechnologies.ductiledb;

import com.tinkerpop.blueprints.Vertex;

public interface DuctileDBVertex extends Vertex {

    @Override
    public Long getId();

    public Iterable<String> getLabels();

    public void addLabel(String label);

    public void removeLabel(String label);

    public boolean hasLabel(String label);

    @Override
    public DuctileDBEdge addEdge(String label, Vertex inVertex);
}
