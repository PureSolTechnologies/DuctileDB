package com.puresoltechnologies.ductiledb.backend;

public class DatabaseConfiguration {

    private String host;
    private int port;
    private String database = "ductiledb";
    private String username;
    private String password;
    private int maxConnections = 20;
    private int minConnections = 5;

    public String getHost() {
	return host;
    }

    public void setHost(String host) {
	this.host = host;
    }

    public int getPort() {
	return port;
    }

    public void setPort(int port) {
	this.port = port;
    }

    public String getDatabase() {
	return database;
    }

    public void setDatabase(String database) {
	this.database = database;
    }

    public String getUsername() {
	return username;
    }

    public void setUsername(String username) {
	this.username = username;
    }

    public String getPassword() {
	return password;
    }

    public void setPassword(String password) {
	this.password = password;
    }

    public String getJdbcUrl() {
	return "jdbc:postgresql://" + host + ":" + port + "/" + database + "?user=" + username + "&password="
		+ password;
    }

    public int getMaxConnections() {
	return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
	this.maxConnections = maxConnections;
    }

    public int getMinConnections() {
	return minConnections;
    }

    public void setMinConnections(int minConnections) {
	this.minConnections = minConnections;
    }

}
