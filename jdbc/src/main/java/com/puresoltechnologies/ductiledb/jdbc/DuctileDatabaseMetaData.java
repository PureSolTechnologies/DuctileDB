package com.puresoltechnologies.ductiledb.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.core.tables.ddl.ColumnDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.core.tables.ddl.NamespaceDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.NamespaceDefinitionImpl;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinitionImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterableImpl;
import com.puresoltechnologies.ductiledb.core.utils.BuildInformation;
import com.puresoltechnologies.ductiledb.engine.io.Bytes;
import com.puresoltechnologies.versioning.Version;

public class DuctileDatabaseMetaData implements DatabaseMetaData, DuctileWrapper {

    private final DuctileConnection connection;
    private final Version version;

    public DuctileDatabaseMetaData(DuctileConnection connection) {
	this.connection = connection;
	this.version = Version.valueOf(BuildInformation.getVersion());
    }

    private TableStore getTableStore() {
	return connection.getDuctileDB().getTableStore();
    }

    @Override
    public void checkClosed() throws SQLException {
	connection.checkClosed();
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
	return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
	return true;
    }

    @Override
    public String getURL() throws SQLException {
	return DuctileDriver.JDBC_DUCTILE_URL_PREFIX + connection.getUrl();
    }

    @Override
    public String getUserName() throws SQLException {
	return "system";
    }

    @Override
    public boolean isReadOnly() throws SQLException {
	return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
	return "Ductile Database";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
	return version.toString();
    }

    @Override
    public String getDriverName() throws SQLException {
	return DuctileDriver.class.getName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
	return version.toString();
    }

    @Override
    public int getDriverMajorVersion() {
	return version.getMajor();
    }

    @Override
    public int getDriverMinorVersion() {
	return version.getMinor();
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
	return true;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
	return true;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
	return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
	return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
	return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
	return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
	return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
	return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
	return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
	return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
	return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
	return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
	return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
	return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
	return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
	return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
	return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
	return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
	return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
	return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
	return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
	return true;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
	return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
	return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
	return "namespace";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
	return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
	return "database";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
	return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
	return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
	return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
	return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
	return Integer.MAX_VALUE;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
	return true;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
	return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxStatements() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
	return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
	return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
	return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
	return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
	return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
	return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
	return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
	return true;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
	return true;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
	    throws SQLException {
	return DuctileResultSet.empty(connection);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
	    String columnNamePattern) throws SQLException {
	return DuctileResultSet.empty(connection);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
	    throws SQLException {
	// TODO add filter
	TableDefinitionImpl tableDefinition = new TableDefinitionImpl("system", "tables",
		"Contains a list of all table in catalog '" + catalog + "'.");
	tableDefinition.addColumn("tables", "TABLE_CAT", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "TABLE_SCHEM", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "TABLE_NAME", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "TABLE_TYPE", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "REMARKS", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "TYPE_CAT", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "TYPE_SCHEME", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "TYPE_NAME", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "SELF_REFERENCING_COL_NAME", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "REF_GENERATION", ColumnType.VARCHAR);

	Map<String, Map<String, String>> tables = new HashMap<>();

	TableStore tableStore = getTableStore();
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	for (NamespaceDefinition namespace : ddl.getNamespaces()) {
	    for (TableDefinition table : ddl.getTables(namespace.getName())) {
		String tableName = namespace.getName() + "." + table.getName();
		Map<String, String> tableMap = tables.get(tableName);
		if (tableMap == null) {
		    tableMap = new HashMap<>();
		    tables.put(tableName, tableMap);
		}
		tableMap.put("TABLE_CAT", "table_store");
		tableMap.put("TABLE_SCHEM", table.getNamespace());
		tableMap.put("TABLE_NAME", table.getName());
		tableMap.put("TABLE_TYPE", namespace.getName().equals("system") ? "SYSTEM TABLE" : "TABLE");
		tableMap.put("REMARKS", table.getDescription());
	    }
	}

	return new DuctileResultSet(connection, new TableRowIterableImpl<>(tables.entrySet(), tableEntry -> {
	    Map<String, String> values = tableEntry.getValue();
	    TableRowImpl tableRow = new TableRowImpl(tableDefinition, null);
	    tableRow.add("TABLE_CAT", Bytes.toBytes("table_store"));
	    tableRow.add("TABLE_SCHEM", Bytes.toBytes(values.get("TABLE_SCHEM")));
	    tableRow.add("TABLE_NAME", Bytes.toBytes(values.get("TABLE_NAME")));
	    tableRow.add("TABLE_TYPE", Bytes.toBytes(values.get("TABLE_TYPE")));
	    tableRow.add("REMARKS", Bytes.toBytes(values.get("REMARKS")));
	    tableRow.add("TYPE_CAT", null);
	    tableRow.add("TYPE_SCHEME", null);
	    tableRow.add("TYPE_NAME", null);
	    tableRow.add("SELF_REFERENCING_COL_NAME", null);
	    tableRow.add("REF_GENERATION", null);
	    return tableRow;
	}));
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
	TableDefinitionImpl tableDefinition = new TableDefinitionImpl("system", "namespaces",
		"Contains a list of all available schemas.");
	tableDefinition.addColumn("schemas", "TABLE_SCHEM", ColumnType.VARCHAR);
	tableDefinition.addColumn("schemas", "TABLE_CATALOG", ColumnType.VARCHAR);

	List<NamespaceDefinition> namespaces = new ArrayList<>();
	namespaces.add(new NamespaceDefinitionImpl("blob_store", "blobs"));
	namespaces.add(new NamespaceDefinitionImpl("graph_store", "ductiledb"));

	TableStore tableStore = getTableStore();
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	Iterable<NamespaceDefinition> namespaceIterable = ddl.getNamespaces();
	namespaces.addAll((Collection<? extends NamespaceDefinition>) namespaceIterable);

	return new DuctileResultSet(connection, new TableRowIterableImpl<>(namespaces, namespace -> {
	    TableRowImpl tableRow = new TableRowImpl(tableDefinition, null);
	    tableRow.add("TABLE_SCHEM", Bytes.toBytes(namespace.getName()));
	    tableRow.add("TABLE_CATALOG", Bytes.toBytes("table_store"));
	    return tableRow;
	}));
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
	TableDefinitionImpl tableDefinition = new TableDefinitionImpl("system", "databases",
		"Contains a list of all available catalogs.");
	tableDefinition.addColumn("databases", "TABLE_CAT", ColumnType.VARCHAR);

	List<String> catalogs = new ArrayList<>();
	catalogs.add("blob_store");
	catalogs.add("graph_store");
	catalogs.add("table_store");

	return new DuctileResultSet(connection, new TableRowIterableImpl<>(catalogs, catalog -> {
	    TableRowImpl tableRow = new TableRowImpl(tableDefinition, null);
	    tableRow.add("TABLE_CAT", Bytes.toBytes(catalog));
	    return tableRow;
	}));
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
	TableDefinitionImpl tableDefinition = new TableDefinitionImpl("system", "namespaces",
		"Contains a list of all supported table types.");
	tableDefinition.addColumn("metadata", "TABLE_TYPE", ColumnType.VARCHAR);

	List<String> tableTypes = new ArrayList<>();
	tableTypes.add("TABLE");
	tableTypes.add("SYSTEM TABLE");
	return new DuctileResultSet(connection, new TableRowIterableImpl<>(tableTypes, tableType -> {
	    TableRowImpl tableRow = new TableRowImpl(tableDefinition, null);
	    tableRow.add("TABLE_TYPE", Bytes.toBytes(tableType));
	    return tableRow;
	}));
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
	    throws SQLException {
	// TODO add filter
	TableDefinitionImpl tableDefinition = new TableDefinitionImpl("system", "tables",
		"Contains a list of all table in catalog '" + catalog + "'.");

	tableDefinition.addColumn("tables", "TABLE_CAT", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "TABLE_SCHEM", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "TABLE_NAME", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "COLUMN_NAME", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "DATA_TYPE", ColumnType.INTEGER);
	tableDefinition.addColumn("tables", "TYPE_NAME", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "COLUMN_SIZE", ColumnType.INTEGER);
	tableDefinition.addColumn("tables", "BUFFER_LENGTH", ColumnType.INTEGER);
	tableDefinition.addColumn("tables", "DECIMAL_DIGITS", ColumnType.INTEGER);
	tableDefinition.addColumn("tables", "NUM_PREC_RADIX", ColumnType.INTEGER);
	tableDefinition.addColumn("tables", "NULLABLE", ColumnType.INTEGER);
	tableDefinition.addColumn("tables", "REMARKS", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "COLUMN_DEF", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "SQL_DATA_TYPE", ColumnType.INTEGER);
	tableDefinition.addColumn("tables", "SQL_DATETIME_SUB", ColumnType.INTEGER);
	tableDefinition.addColumn("tables", "CHAR_OCTET_LENGTH", ColumnType.INTEGER);
	tableDefinition.addColumn("tables", "ORDINAL_POSITION", ColumnType.INTEGER);
	tableDefinition.addColumn("tables", "IS_NULLABLE", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "SCOPE_CATALOG", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "SCOPE_SCHEMA", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "SCOPE_TABLE", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "SOURCE_DATA_TYPE", ColumnType.SHORT);
	tableDefinition.addColumn("tables", "IS_AUTOINCREMENT", ColumnType.VARCHAR);
	tableDefinition.addColumn("tables", "IS_GENERATEDCOLUMN", ColumnType.VARCHAR);

	Map<String, Map<String, Object>> tables = new HashMap<>();

	TableStore tableStore = getTableStore();
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	for (NamespaceDefinition namespace : ddl.getNamespaces()) {
	    for (TableDefinition table : ddl.getTables(namespace.getName())) {
		int position = 0;
		for (ColumnDefinition<?> column : table.getColumnDefinitions()) {
		    position++;
		    String columnName = namespace.getName() + "." + table.getName() + "." + column.getName();
		    Map<String, Object> columnMap = tables.get(columnName);
		    if (columnMap == null) {
			columnMap = new HashMap<>();
			tables.put(columnName, columnMap);
		    }
		    columnMap.put("TABLE_CAT", "table_store");
		    columnMap.put("TABLE_SCHEM", table.getNamespace());
		    columnMap.put("TABLE_NAME", table.getName());
		    columnMap.put("COLUMN_NAME", column.getName());
		    columnMap.put("DATA_TYPE", column.getType().getDataType());
		    columnMap.put("TYPE_NAME", column.getType().getName());
		    // columnMap.put("COLUMN_SIZE", Bytes.toBytes((Integer)
		    // values.get("COLUMN_SIZE")));
		    // columnMap.put("DECIMAL_DIGITS", Bytes.toBytes((Integer)
		    // values.get("DECIMAL_DIGITS")));
		    // columnMap.put("NUM_PREC_RADIX", Bytes.toBytes((Integer)
		    // values.get("NUM_PREC_RADIX")));
		    columnMap.put("COLUMN_SIZE", 0);
		    columnMap.put("DECIMAL_DIGITS", 0);
		    columnMap.put("NUM_PREC_RADIX", 0);
		    columnMap.put("REMARKS", column.getDescription());
		    columnMap.put("CHAR_OCTET_LENGTH", 0);
		    // columnMap.put("CHAR_OCTET_LENGTH",
		    // Bytes.toBytes((Integer)
		    // values.get("CHAR_OCTET_LENGTH")));
		    columnMap.put("ORDINAL_POSITION", position);
		}
	    }
	}

	return new DuctileResultSet(connection, new TableRowIterableImpl<>(tables.entrySet(), tableEntry -> {
	    Map<String, Object> values = tableEntry.getValue();
	    TableRowImpl tableRow = new TableRowImpl(tableDefinition, null);
	    tableRow.add("TABLE_CAT", Bytes.toBytes("table_store"));
	    tableRow.add("TABLE_SCHEM", Bytes.toBytes((String) values.get("TABLE_SCHEM")));
	    tableRow.add("TABLE_NAME", Bytes.toBytes((String) values.get("TABLE_NAME")));
	    tableRow.add("COLUMN_NAME", Bytes.toBytes((String) values.get("COLUMN_NAME")));
	    tableRow.add("DATA_TYPE", Bytes.toBytes((Integer) values.get("DATA_TYPE")));
	    tableRow.add("TYPE_NAME", Bytes.toBytes((String) values.get("TYPE_NAME")));
	    tableRow.add("COLUMN_SIZE", Bytes.toBytes((Integer) values.get("COLUMN_SIZE")));
	    tableRow.add("BUFFER_LENGTH", null);
	    tableRow.add("DECIMAL_DIGITS", Bytes.toBytes((Integer) values.get("DECIMAL_DIGITS")));
	    tableRow.add("NUM_PREC_RADIX", Bytes.toBytes((Integer) values.get("NUM_PREC_RADIX")));
	    tableRow.add("NULLABLE", Bytes.toBytes(columnNullable));
	    tableRow.add("REMARKS", Bytes.toBytes((String) values.get("REMARKS")));
	    tableRow.add("COLUMN_DEF", null);
	    tableRow.add("SQL_DATA_TYPE", null);
	    tableRow.add("SQL_DATETIME_SUB", null);
	    tableRow.add("CHAR_OCTET_LENGTH", Bytes.toBytes((Integer) values.get("CHAR_OCTET_LENGTH")));
	    tableRow.add("ORDINAL_POSITION", Bytes.toBytes((Integer) values.get("ORDINAL_POSITION")));
	    tableRow.add("IS_NULLABLE", Bytes.toBytes(true));
	    tableRow.add("SCOPE_CATALOG", null);
	    tableRow.add("SCOPE_SCHEMA", null);
	    tableRow.add("SCOPE_TABLE", null);
	    tableRow.add("SOURCE_DATA_TYPE", null);
	    tableRow.add("IS_AUTOINCREMENT", Bytes.toBytes("NO"));
	    tableRow.add("IS_GENERATEDCOLUMN", Bytes.toBytes("NO"));

	    return tableRow;
	}));
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
	    throws SQLException {
	// TODO Auto-generated method stub
	return DuctileResultSet.empty(connection);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
	    throws SQLException {
	// TODO Auto-generated method stub
	return DuctileResultSet.empty(connection);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
	    throws SQLException {
	// TODO Auto-generated method stub
	return DuctileResultSet.empty(connection);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
	return DuctileResultSet.empty(connection);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
	return DuctileResultSet.empty(connection);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
	return DuctileResultSet.empty(connection);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
	    String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
	// TODO Auto-generated method stub
	return DuctileResultSet.empty(connection);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
	    throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
	return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
	return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
	    throws SQLException {
	return DuctileResultSet.empty(connection);
    }

    @Override
    public Connection getConnection() throws SQLException {
	return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
	    String attributeNamePattern) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
	return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
	return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
	return Version.valueOf(BuildInformation.getVersion()).getMajor();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
	return Version.valueOf(BuildInformation.getVersion()).getMinor();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
	return Version.valueOf(BuildInformation.getVersion()).getMajor();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
	return Version.valueOf(BuildInformation.getVersion()).getMinor();
    }

    @Override
    public int getSQLStateType() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
	TableDefinitionImpl tableDefinition = new TableDefinitionImpl("system", "namespaces",
		"Contains a list of al client info properties.");
	tableDefinition.addColumn("client_info", "NAME", ColumnType.VARCHAR);
	tableDefinition.addColumn("client_info", "MAX_LEN", ColumnType.INTEGER);
	tableDefinition.addColumn("client_info", "DEFAULT_VALUE", ColumnType.VARCHAR);
	tableDefinition.addColumn("client_info", "DESCRIPTION", ColumnType.VARCHAR);

	Properties clientInfo = connection.getClientInfo();
	TableRowIterable iterable = new TableRowIterableImpl<>(clientInfo.entrySet(), entry -> {
	    TableRowImpl tableRow = new TableRowImpl(tableDefinition, null);
	    tableRow.add("NAME", Bytes.toBytes(entry.getKey().toString()));
	    tableRow.add("MAX_LEN", Bytes.toBytes(Integer.MAX_VALUE));
	    tableRow.add("DEFAULT_VALUE", Bytes.toBytes("n/a"));
	    tableRow.add("DESCRIPTION", Bytes.toBytes("n/a"));

	    return tableRow;
	});
	return new DuctileResultSet(connection, iterable);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
	    throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
	    String columnNamePattern) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
	    String columnNamePattern) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

}
