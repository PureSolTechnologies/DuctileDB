package com.puresoltechnologies.ductiledb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;

import com.puresoltechnologies.ductiledb.core.utils.BuildInformation;

public class DuctileJDBCMainClass {

    public static void main(String[] args) {
	System.out.println("DuctileDB JDBC Driver v" + BuildInformation.getVersion());
	if (args.length != 1) {
	    System.out.println("usage: <jdbc_url>");
	    return;
	}
	try {
	    Class.forName(DuctileDriver.class.getName());
	    Connection connection = DriverManager.getConnection(args[0]);
	    connection.close();
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}
    }

}
