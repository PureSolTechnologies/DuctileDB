package com.puresoltechnologies.ductiledb.api.tables.dml;

import com.puresoltechnologies.commons.misc.io.CloseableIterable;

/**
 * This interface is used to iterate through table rows.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface TableRowIterable extends CloseableIterable<TableRow> {

}
