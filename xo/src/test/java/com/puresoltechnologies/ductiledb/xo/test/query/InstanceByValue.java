package com.puresoltechnologies.ductiledb.xo.test.query;

import com.puresoltechnologies.ductiledb.xo.api.annotation.Query;

@Query(value = "g.V().hasLabel('A').has('value', {value})", name = "a")
public interface InstanceByValue {

    A getA();

}
