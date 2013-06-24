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

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parallelai.spyglass.jdbc.db.DBConfiguration;

import java.io.IOException;
import java.sql.*;
import java.util.*;

/**
 * Class JDBCTap is a {@link Tap} sub-class that provides read and write access to a RDBMS via JDBC drivers.
 * <p/>
 * This Tap fully supports TABLE DROP and CREATE when given a {@link TableDesc} instance.
 * <p/>
 * When using {@link SinkMode#UPDATE}, Cascading is instructed to not delete the resource (drop the Table)
 * and assumes its safe to begin sinking data into it. The {@link JDBCScheme} is responsible for
 * deciding if/when to perform an UPDATE instead of an INSERT.
 * <p/>
 * Both INSERT and UPDATE are supported through the JDBCScheme.
 * <p/>
 * By sub-classing JDBCScheme, {@link com.twitter.maple.jdbc.db.DBInputFormat}, and {@link com.twitter.maple.jdbc.db.DBOutputFormat},
 * specific vendor features can be supported.
 * <p/>
 * Use {@link #setBatchSize(int)} to set the number of INSERT/UPDATES should be grouped together before being
 * executed. The default vaue is 1,000.
 * <p/>
 * Use {@link #executeQuery(String, int)} or {@link #executeUpdate(String)} to invoke SQL statements against
 * the underlying Table.
 * <p/>
 * Note that all classes under the {@link com.twitter.maple.jdbc.db} package originated from the Hadoop project and
 * retain their Apache 2.0 license though they have been heavily modified to support INSERT/UPDATE and
 * vendor specialization, and a number of other features like 'limit'.
 *
 * @see JDBCScheme
 * @see com.twitter.maple.jdbc.db.DBInputFormat
 * @see com.twitter.maple.jdbc.db.DBOutputFormat
 */
public class JDBCTap extends Tap<JobConf, RecordReader, OutputCollector> {
    /** Field LOG */
    private static final Logger LOG = LoggerFactory.getLogger(JDBCTap.class);

    private final String id = UUID.randomUUID().toString();

    /** Field connectionUrl */
    String connectionUrl;
    /** Field username */
    String username;
    /** Field password */
    String password;
    /** Field driverClassName */
    String driverClassName;
    /** Field tableDesc */
    TableDesc tableDesc;
    /** Field batchSize */
    int batchSize = 1000;
    /** Field concurrentReads */
    int concurrentReads = 0;

    /**
     * Constructor JDBCTap creates a new JDBCTap instance.
     * <p/>
     * Use this constructor for connecting to existing tables that will be read from, or will be inserted/updated
     * into. By default it uses {@link SinkMode#UPDATE}.
     *
     * @param connectionUrl   of type String
     * @param username        of type String
     * @param password        of type String
     * @param driverClassName of type String
     * @param tableName       of type String
     * @param scheme          of type JDBCScheme
     */
    public JDBCTap( String connectionUrl, String username, String password, String driverClassName, String tableName, JDBCScheme scheme ) {
        this( connectionUrl, username, password, driverClassName, new TableDesc( tableName ), scheme, SinkMode.UPDATE );
    }

    /**
     * Constructor JDBCTap creates a new JDBCTap instance.
     *
     * @param connectionUrl   of type String
     * @param driverClassName of type String
     * @param tableDesc       of type TableDesc
     * @param scheme          of type JDBCScheme
     * @param sinkMode        of type SinkMode
     */
    public JDBCTap( String connectionUrl, String driverClassName, TableDesc tableDesc, JDBCScheme scheme, SinkMode sinkMode ) {
        this( connectionUrl, null, null, driverClassName, tableDesc, scheme, sinkMode );
    }

    /**
     * Constructor JDBCTap creates a new JDBCTap instance.
     * <p/>
     * Use this constructor for connecting to existing tables that will be read from, or will be inserted/updated
     * into. By default it uses {@link SinkMode#UPDATE}.
     *
     * @param connectionUrl   of type String
     * @param username        of type String
     * @param password        of type String
     * @param driverClassName of type String
     * @param tableDesc       of type TableDesc
     * @param scheme          of type JDBCScheme
     */
    public JDBCTap( String connectionUrl, String username, String password, String driverClassName, TableDesc tableDesc, JDBCScheme scheme ) {
        this( connectionUrl, username, password, driverClassName, tableDesc, scheme, SinkMode.UPDATE );
    }

    /**
     * Constructor JDBCTap creates a new JDBCTap instance.
     *
     * @param connectionUrl   of type String
     * @param username        of type String
     * @param password        of type String
     * @param driverClassName of type String
     * @param tableDesc       of type TableDesc
     * @param scheme          of type JDBCScheme
     * @param sinkMode        of type SinkMode
     */
    public JDBCTap( String connectionUrl, String username, String password, String driverClassName, TableDesc tableDesc, JDBCScheme scheme, SinkMode sinkMode ) {
        super( scheme, sinkMode );
        this.connectionUrl = connectionUrl;
        this.username = username;
        this.password = password;
        this.driverClassName = driverClassName;
        this.tableDesc = tableDesc;

        if( tableDesc.getColumnDefs() == null && sinkMode != SinkMode.UPDATE )
            throw new IllegalArgumentException( "cannot have sink mode REPLACE or KEEP without TableDesc column defs, use UPDATE mode" );

        if( sinkMode != SinkMode.UPDATE )
            LOG.warn( "using sink mode: {}, consider UPDATE to prevent DROP TABLE from being called during Flow or Cascade setup", sinkMode );
    }

    /**
     * Constructor JDBCTap creates a new JDBCTap instance.
     * <p/>
     * Use this constructor for connecting to existing tables that will be read from, or will be inserted/updated
     * into. By default it uses {@link SinkMode#UPDATE}.
     *
     * @param connectionUrl   of type String
     * @param driverClassName of type String
     * @param tableDesc       of type TableDesc
     * @param scheme          of type JDBCScheme
     */
    public JDBCTap( String connectionUrl, String driverClassName, TableDesc tableDesc, JDBCScheme scheme ) {
        this( connectionUrl, driverClassName, tableDesc, scheme, SinkMode.UPDATE );
    }

    /**
     * Constructor JDBCTap creates a new JDBCTap instance that may only used as a data source.
     *
     * @param connectionUrl   of type String
     * @param username        of type String
     * @param password        of type String
     * @param driverClassName of type String
     * @param scheme          of type JDBCScheme
     */
    public JDBCTap( String connectionUrl, String username, String password, String driverClassName, JDBCScheme scheme ) {
        super( scheme );
        this.connectionUrl = connectionUrl;
        this.username = username;
        this.password = password;
        this.driverClassName = driverClassName;
    }

    /**
     * Constructor JDBCTap creates a new JDBCTap instance.
     *
     * @param connectionUrl   of type String
     * @param driverClassName of type String
     * @param scheme          of type JDBCScheme
     */
    public JDBCTap( String connectionUrl, String driverClassName, JDBCScheme scheme ) {
        this( connectionUrl, null, null, driverClassName, scheme );
    }

    /**
     * Method getTableName returns the tableName of this JDBCTap object.
     *
     * @return the tableName (type String) of this JDBCTap object.
     */
    public String getTableName() {
        return tableDesc.tableName;
    }

    /**
     * Method setBatchSize sets the batchSize of this JDBCTap object.
     *
     * @param batchSize the batchSize of this JDBCTap object.
     */
    public void setBatchSize( int batchSize ) {
        this.batchSize = batchSize;
    }

    /**
     * Method getBatchSize returns the batchSize of this JDBCTap object.
     *
     * @return the batchSize (type int) of this JDBCTap object.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Method getConcurrentReads returns the concurrentReads of this JDBCTap object.
     * <p/>
     * This value specifies the number of concurrent selects and thus the number of mappers
     * that may be used. A value of -1 uses the job default.
     *
     * @return the concurrentReads (type int) of this JDBCTap object.
     */
    public int getConcurrentReads() {
        return concurrentReads;
    }

    /**
     * Method setConcurrentReads sets the concurrentReads of this JDBCTap object.
     * <p/>
     * This value specifies the number of concurrent selects and thus the number of mappers
     * that may be used. A value of -1 uses the job default.
     *
     * @param concurrentReads the concurrentReads of this JDBCTap object.
     */
    public void setConcurrentReads( int concurrentReads ) {
        this.concurrentReads = concurrentReads;
    }

    /**
     * Method getPath returns the path of this JDBCTap object.
     *
     * @return the path (type Path) of this JDBCTap object.
     */
    public Path getPath() {
        return new Path( getJDBCPath() );
    }

    @Override
    public String getIdentifier() {
        return getJDBCPath() + this.id;
    }


    public String getJDBCPath() {
        return "jdbc:/" + connectionUrl.replaceAll( ":", "_" );
    }

    public boolean isWriteDirect() {
        return true;
    }

    private JobConf getSourceConf( FlowProcess<JobConf> flowProcess, JobConf conf, String property )
        throws IOException {
      //  Map<String, String> priorConf = HadoopUtil.deserializeBase64( property, conf, true );
      //  return flowProcess.mergeMapIntoConfig( conf, priorConf );
    	
    	return null;
    }

    @Override
    public TupleEntryIterator openForRead( FlowProcess<JobConf> flowProcess, RecordReader input ) throws IOException {
        // input may be null when this method is called on the client side or cluster side when accumulating
        // for a HashJoin
        return new HadoopTupleEntrySchemeIterator( flowProcess, this, input );
    }

    @Override
    public TupleEntryCollector openForWrite( FlowProcess<JobConf> flowProcess, OutputCollector output ) throws IOException {
        if( !isSink() )
            throw new TapException( "this tap may not be used as a sink, no TableDesc defined" );

        LOG.info("Creating JDBCTapCollector output instance");
        JDBCTapCollector jdbcCollector = new JDBCTapCollector( flowProcess, this );

        jdbcCollector.prepare();

        return jdbcCollector;
    }

    @Override
    public boolean isSink()
    {
        return tableDesc != null;
    }

    @Override
    public void sourceConfInit( FlowProcess<JobConf> process, JobConf conf )
    {
        // a hack for MultiInputFormat to see that there is a child format
        FileInputFormat.setInputPaths( conf, getPath() );

        if( username == null )
            DBConfiguration.configureDB(conf, driverClassName, connectionUrl);
        else
            DBConfiguration.configureDB( conf, driverClassName, connectionUrl, username, password );

        super.sourceConfInit( process, conf );
    }

    @Override
    public void sinkConfInit( FlowProcess<JobConf> process, JobConf conf )
    {
        if( !isSink() )
            return;

        // do not delete if initialized from within a task
        try {
            if( isReplace() && conf.get( "mapred.task.partition" ) == null && !deleteResource( conf ) )
                throw new TapException( "unable to drop table: " + tableDesc.getTableName() );

            if( !createResource( conf ) )
                throw new TapException( "unable to create table: " + tableDesc.getTableName() );
        } catch(IOException e) {
            throw new TapException( "error while trying to modify table: " + tableDesc.getTableName() );
        }

        if( username == null )
            DBConfiguration.configureDB( conf, driverClassName, connectionUrl );
        else
            DBConfiguration.configureDB( conf, driverClassName, connectionUrl, username, password );

        super.sinkConfInit( process, conf );
    }

    private Connection createConnection()
    {
        try
        {
            LOG.info( "creating connection: {}", connectionUrl );

            Class.forName( driverClassName );

            Connection connection = null;

            if( username == null )
                connection = DriverManager.getConnection( connectionUrl );
            else
                connection = DriverManager.getConnection( connectionUrl, username, password );

            connection.setAutoCommit( false );

            return connection;
        }
        catch( ClassNotFoundException exception )
        {
            throw new TapException( "unable to load driver class: " + driverClassName, exception );
        }
        catch( SQLException exception )
        {
            throw new TapException( "unable to open connection: " + connectionUrl, exception );
        }
    }

    /**
     * Method executeUpdate allows for ad-hoc update statements to be sent to the remote RDBMS. The number of
     * rows updated will be returned, if applicable.
     *
     * @param updateString of type String
     * @return int
     */
    public int executeUpdate( String updateString )
    {
        Connection connection = null;
        int result;

        try
        {
            connection = createConnection();

            try
            {
                LOG.info( "executing update: {}", updateString );

                Statement statement = connection.createStatement();

                result = statement.executeUpdate( updateString );

                connection.commit();
                statement.close();
            }
            catch( SQLException exception )
            {
                throw new TapException( "unable to execute update statement: " + updateString, exception );
            }
        }
        finally
        {
            try
            {
                if( connection != null )
                    connection.close();
            }
            catch( SQLException exception )
            {
                // ignore
                LOG.warn( "ignoring connection close exception", exception );
            }
        }

        return result;
    }

    /**
     * Method executeQuery allows for ad-hoc queries to be sent to the remove RDBMS. A value
     * of -1 for returnResults will return a List of all results from the query, a value of 0 will return an empty List.
     *
     * @param queryString   of type String
     * @param returnResults of type int
     * @return List
     */
    public List<Object[]> executeQuery( String queryString, int returnResults )
    {
        Connection connection = null;
        List<Object[]> result = Collections.emptyList();

        try
        {
            connection = createConnection();

            try
            {
                LOG.info( "executing query: {}", queryString );

                Statement statement = connection.createStatement();

                ResultSet resultSet = statement.executeQuery( queryString ); // we don't care about results

                if( returnResults != 0 )
                    result = copyResultSet( resultSet, returnResults == -1 ? Integer.MAX_VALUE : returnResults );

                connection.commit();
                statement.close();
            }
            catch( SQLException exception )
            {
                throw new TapException( "unable to execute query statement: " + queryString, exception );
            }
        }
        finally
        {
            try
            {
                if( connection != null )
                    connection.close();
            }
            catch( SQLException exception )
            {
                // ignore
                LOG.warn( "ignoring connection close exception", exception );
            }
        }

        return result;
    }

    private List<Object[]> copyResultSet( ResultSet resultSet, int length ) throws SQLException
    {
        List<Object[]> results = new ArrayList<Object[]>( length );
        int size = resultSet.getMetaData().getColumnCount();

        int count = 0;

        while( resultSet.next() && count < length )
        {
            count++;

            Object[] row = new Object[size];

            for( int i = 0; i < row.length; i++ )
                row[ i ] = resultSet.getObject( i + 1 );

            results.add( row );
        }

        return results;
    }

    @Override
    public boolean createResource( JobConf conf ) throws IOException
    {
        if( resourceExists( conf ) )
            return true;

        try
        {
            LOG.info( "creating table: {}", tableDesc.tableName );

            executeUpdate( tableDesc.getCreateTableStatement() );
        }
        catch( TapException exception )
        {
            LOG.warn( "unable to create table: {}", tableDesc.tableName );
            LOG.warn( "sql failure", exception.getCause() );

            return false;
        }

        return resourceExists( conf );
    }

    @Override
    public boolean deleteResource( JobConf conf ) throws IOException
    {
        if( !isSink() )
            return false;

        if( !resourceExists( conf ) )
            return true;

        try
        {
            LOG.info( "deleting table: {}", tableDesc.tableName );

            executeUpdate( tableDesc.getTableDropStatement() );
        }
        catch( TapException exception )
        {
            LOG.warn( "unable to drop table: {}", tableDesc.tableName );
            LOG.warn( "sql failure", exception.getCause() );

            return false;
        }

        return !resourceExists( conf );
    }

    @Override
    public boolean resourceExists( JobConf conf ) throws IOException
    {
        if( !isSink() )
            return true;

        try
        {
            LOG.info( "test table exists: {}", tableDesc.tableName );

            executeQuery( tableDesc.getTableExistsQuery(), 0 );
        }
        catch( TapException exception )
        {
            return false;
        }

        return true;
    }

    @Override
    public long getModifiedTime( JobConf conf ) throws IOException
    {
        return System.currentTimeMillis();
    }

    @Override
    public String toString()
    {
        return "JDBCTap{" + "connectionUrl='" + connectionUrl + '\'' + ", driverClassName='" + driverClassName + '\'' + ", tableDesc=" + tableDesc + '}';
    }

    @Override
    public boolean equals( Object object )
    {
        if( this == object )
            return true;
        if( !( object instanceof JDBCTap ) )
            return false;
        if( !super.equals( object ) )
            return false;

        JDBCTap jdbcTap = (JDBCTap) object;

        if( connectionUrl != null ? !connectionUrl.equals( jdbcTap.connectionUrl ) : jdbcTap.connectionUrl != null )
            return false;
        if( driverClassName != null ? !driverClassName.equals( jdbcTap.driverClassName ) : jdbcTap.driverClassName != null )
            return false;
        if( password != null ? !password.equals( jdbcTap.password ) : jdbcTap.password != null )
            return false;
        if( tableDesc != null ? !tableDesc.equals( jdbcTap.tableDesc ) : jdbcTap.tableDesc != null )
            return false;
        if( username != null ? !username.equals( jdbcTap.username ) : jdbcTap.username != null )
            return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + ( connectionUrl != null ? connectionUrl.hashCode() : 0 );
        result = 31 * result + ( username != null ? username.hashCode() : 0 );
        result = 31 * result + ( password != null ? password.hashCode() : 0 );
        result = 31 * result + ( driverClassName != null ? driverClassName.hashCode() : 0 );
        result = 31 * result + ( tableDesc != null ? tableDesc.hashCode() : 0 );
        result = 31 * result + batchSize;
        return result;
    }
}
