package de.hshn.mi.pdbg.basicservice;

import java.sql.SQLException;

/**
 * The Main class contains the entry point for the application. It creates tables for the database.
 * This class is typically used to start the application.
 */
public class Main {
    /**
     * The main method creates an instance of the DBCreator class and uses it to create a database.
     * The database URL, username, and password are provided as arguments to the createDatabase method.
     */
    public static void main(String[] args) {

        DBCreator dbCreator = new DBCreator();
        dbCreator.createDatabase("jdbc:postgresql:DB_Aufgabe2", "postgres", "linkin420");
        System.out.println("hi");
    }
}
