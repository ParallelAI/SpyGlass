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

import cascading.tuple.Tuple;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import parallelai.spyglass.jdbc.db.DBWritable;

public class TupleRecord implements DBWritable {
    private Tuple tuple;

    public TupleRecord() {
    }

    public TupleRecord( Tuple tuple ) {
        this.tuple = tuple;
    }

    public void setTuple( Tuple tuple ) {
        this.tuple = tuple;
    }

    public Tuple getTuple() {
        return tuple;
    }

    public void write( PreparedStatement statement ) throws SQLException {
        for( int i = 0; i < tuple.size(); i++ ) {
        	//System.out.println("Insert Tuple => " + " statement.setObject( " + (i + 1) + "," + tuple.get( i ));
            statement.setObject( i + 1, tuple.get( i ) );
        }
        boolean test = true;
        if (test) {
	        for( int i = 1; i < tuple.size(); i++ ) {
	        	//System.out.println("Update Tuple => " + " statement.setObject( " + (i + tuple.size()) + "," + tuple.get( i ));
	            statement.setObject( i + tuple.size(), tuple.get( i ) );
	        }
        }
        
    }

    public void readFields( ResultSet resultSet ) throws SQLException {
        tuple = new Tuple();

        for( int i = 0; i < resultSet.getMetaData().getColumnCount(); i++ )
            tuple.add( (Comparable) resultSet.getObject( i + 1 ) );
    }

}
