package com.puresoltechnologies.ductiledb;

import com.tinkerpop.blueprints.Vertex;

public interface DuctileDBVertex extends Vertex {

    public Iterable<String> getLabels();

    public void addLabel(String label);

    public void removeLabel(String label);

    public boolean hasLabel(String label);

}
