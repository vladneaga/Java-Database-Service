package de.hshn.mi.pdbg.basicservice;

import de.hshn.mi.pdbg.schema.SchemaGenerator;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Implementation of the SchemaGenerator interface for creating database schema.
 * This class provides methods to generate and execute SQL DDL statements for
 * creating tables and sequences in a PostgreSQL database.
 * It also contains constants for database connection parameters and SQL DDL statements.
 * Note: The actual database creation process is specific to PostgreSQL.
 *
 * @author Neaga Vlad, Abdul Satar Amiri, Hussein Radujew
 * @version 1.0
 */

public final class DBCreator implements SchemaGenerator {

    static final String DB_URL = "jdbc:postgresql://postgres/pdbg-a2";
    static final String USER = "postgres";
    static final String PASS = "postgres";

    protected static final String [ ] SQL_DDL_STATEMENTS = {

        "SET WRITE_DELAY FALSE", // S p e c i f i c t o HsqlDB
        "CREATE TABLE Person (\n" +
                    " ID BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,\n" +
                    " Vorname VARCHAR(50),\n" +
                    " Nachname VARCHAR(50),\n" +
                    " Geburtsdatum Date\n" +
                    ");\n" +
                    "CREATE TABLE Patient(\n" +
                    " ID BIGINT PRIMARY KEY references Person(ID)\n" +
                    "on delete cascade on update cascade,\n" +
                    " Krankenkasse VARCHAR(50),\n" +
                    " Versicherungsnummer VARCHAR(50)\n" +
                    ");\n" +
                    "CREATE TABLE Station (\n" +
                    " ID BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,\n" +
                    " Bezeichnung VARCHAR(50),\n" +
                    " Bettenzahl INT\n" +
                    ");\n" +
                    "CREATE TABLE Aufenthalt (\n" +
                    " ID BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,\n" +
                    " PID BIGINT references Patient(ID)\n" +
                    "on delete set null\n" +
                    "on update cascade,\n" +
                    " SID BIGINT references Station(ID)\n" +
                    "on delete set null\n" +
                    "on update cascade,\n" +
                    " Aufnahmedatum Date,\n" +
                    " Entlassdatum Date\n" +
                    ");\n",
        "SHUTDOWN" // S p e c i f i c t o HsqlDB
    };

    @Override
    public boolean createDatabase(String s, String s1, String s2) {
        try (Connection conn = createConnection(s, s1, s2

                );
            Statement stmt = conn.createStatement();
        ) {
            //stmt.executeUpdate(SQL_DDL_STATEMENTS[0]);
            stmt.executeUpdate(SQL_DDL_STATEMENTS[1]);
            // stmt.executeUpdate(SQL_DDL_STATEMENTS[2]);

            System.out.println("Database created successfully...");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    /**
     * This method creates a new connection the database.
     *
     * @param jdbcURL   represents the connection String of the database
     * @param user      login credentials
     * @param password  of the database
     * @return          a Connection object representing the database connection
     * @throws ClassNotFoundException if the JDBC driver class is not found
     * @throws SQLException           if a database access error occurs
     */
    protected Connection createConnection(String jdbcURL, String user, String password)
            throws ClassNotFoundException, SQLException {
        Class.forName(org.postgresql.Driver.class.getName());
        return DriverManager.getConnection(jdbcURL, user, password);

    }


}
