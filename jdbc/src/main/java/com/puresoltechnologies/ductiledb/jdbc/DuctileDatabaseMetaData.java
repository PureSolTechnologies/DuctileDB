package com.puresoltechnologies.ductiledb.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.core.tables.ddl.DataDefinitionLanguage;
import com.puresoltechnologies.ductiledb.core.tables.ddl.NamespaceDefinition;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinitionImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterableImpl;
import com.puresoltechnologies.ductiledb.core.utils.BuildInformation;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.utils.EmptyIterable;
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
	// TODO Auto-generated method stub
	return null;
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
	// TODO Auto-generated method stub
	return null;
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
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
	// TODO Auto-generated method stub
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
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
	return true;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
	// TODO Auto-generated method stub
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
	return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
	return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
	return "namespace";
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
	// TODO Auto-generated method stub
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
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
	return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
	return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
	return Integer.MAX_VALUE;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
	return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxStatements() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
	return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
	    throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
	    String columnNamePattern) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
	    throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
	TableDefinitionImpl tableDefinition = new TableDefinitionImpl("system", "namespaces");
	tableDefinition.addColumn("metadata", "TABLE_CAT", ColumnType.VARCHAR);

	TableStore tableStore = getTableStore();
	DataDefinitionLanguage ddl = tableStore.getDataDefinitionLanguage();
	Iterable<NamespaceDefinition> namespaces = ddl.getNamespaces();
	return new DuctileResultSet(connection, new TableRowIterableImpl<>(namespaces, namespace -> {
	    TableRowImpl tableRow = new TableRowImpl(tableDefinition, null);
	    tableRow.add("TABLE_CAT", Bytes.toBytes(namespace.getName()));
	    return tableRow;
	}));
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
	TableDefinitionImpl tableDefinition = new TableDefinitionImpl("system", "namespaces");
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
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
	    throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
	    throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
	    throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
	    String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
	// TODO Auto-generated method stub
	return null;
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
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
	// TODO Auto-generated method stub
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
	TableDefinitionImpl tableDefinition = new TableDefinitionImpl("system", "namespaces");
	tableDefinition.addColumn("metadata", "TYPE_CAT", ColumnType.VARCHAR);
	tableDefinition.addColumn("metadata", "TYPE_SCHEM", ColumnType.VARCHAR);
	tableDefinition.addColumn("metadata", "TYPE_NAME", ColumnType.VARCHAR);
	tableDefinition.addColumn("metadata", "CLASS_NAME", ColumnType.VARCHAR);
	tableDefinition.addColumn("metadata", "DATA_TYPE", ColumnType.INTEGER);
	tableDefinition.addColumn("metadata", "REMARKS", ColumnType.VARCHAR);
	tableDefinition.addColumn("metadata", "BASE_TYPE", ColumnType.SHORT);

	Iterable<NamespaceDefinition> namespaces = new EmptyIterable<>();
	return new DuctileResultSet(connection, new TableRowIterableImpl<>(namespaces, namespace -> {
	    return new TableRowImpl(tableDefinition, null);
	}));
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
	// TODO Auto-generated method stub
	return null;
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
