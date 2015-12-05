package com.puresoltechnologies.ductiledb.api;

import java.util.Set;

public interface DuctileDBElement {

    public Long getId();

    public Set<String> getPropertyKeys();

    public void setProperty(String key, Object value);

    public <T> T getProperty(String key);

    public <T> T removeProperty(String key);

    public void remove();

}
