/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package parallelai.spyglass.jdbc;

import cascading.util.Util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class TableDesc describes a SQL based table, this description is used by the {@link JDBCTap} when
 * creating a missing table.
 *
 * @see JDBCTap
 * @see JDBCScheme
 */
public class TableDesc implements Serializable {
    /** Field tableName */
    String tableName;
    /** Field columnNames */
    String[] columnNames;
    /** Field columnDefs */
    String[] columnDefs;
    /** Field primaryKeys */
    String[] primaryKeys;

    /**
     * Constructor TableDesc creates a new TableDesc instance.
     *
     * @param tableName of type String
     */
    public TableDesc( String tableName ) {
        this.tableName = tableName;
    }

    /**
     * Constructor TableDesc creates a new TableDesc instance.
     *
     * @param tableName   of type String
     * @param columnNames of type String[]
     * @param columnDefs  of type String[]
     * @param primaryKeys of type String
     */
    public TableDesc( String tableName, String[] columnNames, String[] columnDefs, String[] primaryKeys ) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnDefs = columnDefs;
        this.primaryKeys = primaryKeys;
    }

    public String getTableName() {
        return tableName;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public String[] getColumnDefs() {
        return columnDefs;
    }

    public String[] getPrimaryKeys() {
        return primaryKeys;
    }

    /**
     * Method getTableCreateStatement returns the tableCreateStatement of this TableDesc object.
     *
     * @return the tableCreateStatement (type String) of this TableDesc object.
     */
    public String getCreateTableStatement() {
        List<String> createTableStatement = new ArrayList<String>();

        createTableStatement = addCreateTableBodyTo( createTableStatement );

        return String.format( getCreateTableFormat(), tableName, Util.join( createTableStatement, ", " ) );
    }

    protected List<String> addCreateTableBodyTo( List<String> createTableStatement ) {
        createTableStatement = addDefinitionsTo( createTableStatement );
        createTableStatement = addPrimaryKeyTo( createTableStatement );

        return createTableStatement;
    }

    protected String getCreateTableFormat() {
        return "CREATE TABLE %s ( %s )";
    }

    protected List<String> addDefinitionsTo( List<String> createTableStatement ) {
        for( int i = 0; i < columnNames.length; i++ ) {
            String columnName = columnNames[ i ];
            String columnDef = columnDefs[ i ];

            createTableStatement.add( columnName + " " + columnDef );
        }

        return createTableStatement;
    }

    protected List<String> addPrimaryKeyTo( List<String> createTableStatement ) {
        if( hasPrimaryKey() )
            createTableStatement.add( String.format( "PRIMARY KEY( %s )", Util.join( primaryKeys, ", " ) ) );

        return createTableStatement;
    }

    /**
     * Method getTableDropStatement returns the tableDropStatement of this TableDesc object.
     *
     * @return the tableDropStatement (type String) of this TableDesc object.
     */
    public String getTableDropStatement() {
        return String.format( getDropTableFormat(), tableName );
    }

    protected String getDropTableFormat() {
        return "DROP TABLE %s";
    }

    /**
     * Method getTableExistsQuery returns the tableExistsQuery of this TableDesc object.
     *
     * @return the tableExistsQuery (type String) of this TableDesc object.
     */
    public String getTableExistsQuery() {
        return String.format( "select 1 from %s where 1 = 0", tableName );
    }

    private boolean hasPrimaryKey() {
        return primaryKeys != null && primaryKeys.length != 0;
    }

    @Override
    public String toString() {
        return "TableDesc{" + "tableName='" + tableName + '\'' + ", columnNames=" + ( columnNames == null ? null : Arrays.asList( columnNames ) ) + ", columnDefs=" + ( columnDefs == null ? null : Arrays.asList( columnDefs ) ) + ", primaryKeys=" + ( primaryKeys == null ? null : Arrays.asList( primaryKeys ) ) + '}';
    }

    @Override
    public boolean equals( Object object ) {
        if( this == object )
            return true;
        if( !( object instanceof TableDesc ) )
            return false;

        TableDesc tableDesc = (TableDesc) object;

        if( !Arrays.equals( columnDefs, tableDesc.columnDefs ) )
            return false;
        if( !Arrays.equals( columnNames, tableDesc.columnNames ) )
            return false;
        if( !Arrays.equals( primaryKeys, tableDesc.primaryKeys ) )
            return false;
        if( tableName != null ? !tableName.equals( tableDesc.tableName ) : tableDesc.tableName != null )
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tableName != null ? tableName.hashCode() : 0;
        result = 31 * result + ( columnNames != null ? Arrays.hashCode( columnNames ) : 0 );
        result = 31 * result + ( columnDefs != null ? Arrays.hashCode( columnDefs ) : 0 );
        result = 31 * result + ( primaryKeys != null ? Arrays.hashCode( primaryKeys ) : 0 );
        return result;
    }
}
