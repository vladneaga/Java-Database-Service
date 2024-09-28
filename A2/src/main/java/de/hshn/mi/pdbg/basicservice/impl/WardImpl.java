package de.hshn.mi.pdbg.basicservice.impl;

import de.hshn.mi.pdbg.PersistentObject;
import de.hshn.mi.pdbg.basicservice.BasicDBService;
import de.hshn.mi.pdbg.basicservice.Ward;
import de.hshn.mi.pdbg.basicservice.jdbc.AbstractPersistentJDBCObject;
import de.hshn.mi.pdbg.basicservice.services.BasicDBServiceImpl;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Implementation of the Ward interface. Represents a ward in the medical database service.
 * Extends AbstractPersistentJDBCObject for database persistence.
 * <p>
 * This class provides methods to set and retrieve ward information such as name and number of beds.
 * </p>
 *
 * @author Neaga Vlad, Abdul Satar Amiri, Hussein Radujew
 * @version 1.0
 */

public class WardImpl extends AbstractPersistentJDBCObject implements Ward {

    private String name;
    private int numberOfBeds;

    /**
     * Constructs a new Ward-object with the specified database service. The ID contains the default value.
     *
     * @param basicDBService        service for creating persistable objects and storing, fetching, and deleting such
     *                              objects from a persistent store
     */
    public WardImpl(BasicDBService basicDBService) {
        super(basicDBService, PersistentObject.INVALID_OBJECT_ID);
    }

    /**
     * Constructs a new Ward-object with the specified database service, ID, number of beds, and name.
     *
     * @param basicDBService        service for creating persistable objects and storing, fetching, and deleting such
     *                              objects from a persistent store.
     * @param id                    The unique identifier of the ward.
     * @param numberOfBeds          The number of beds in the ward.
     * @param name                  The name of the ward.
     */

    public WardImpl(BasicDBService basicDBService, Long id, int numberOfBeds, String name) {
        super(basicDBService, id);
        this.name = name;
        this.numberOfBeds = numberOfBeds;
    }

    @Override
    public int getNumberOfBeds() {
        return numberOfBeds;
    }

    @Override
    protected void setObjectID(long id) {
        super.setObjectID(id);
    }

    @Override
    public void setNumberOfBeds(int number) {
        if (number <= 0) {
            throw new AssertionError("Number of beds must be greater than zero");
        }
        this.numberOfBeds = number;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new AssertionError("Name must not be null or empty");
        }
        this.name = name.trim();
    }

    @Override
    public long getObjectID() {
        return super.getObjectID();
    }

    @Override
    public long store(Connection connection) throws SQLException {
        if (connection.isClosed()) {
            BasicDBServiceImpl service = (BasicDBServiceImpl) getBasicDBService();
            connection = DriverManager.getConnection(service.getDB_URL(), service.getUSER(), service.getPASS());
        }
        if (this.isPersistent()) {
            String updateQuery = "UPDATE Station SET Bezeichnung = ?, Bettenzahl = ? WHERE ID = ?";
            try (PreparedStatement statement = connection.prepareStatement(updateQuery)) {
                statement.setString(1, name);
                statement.setInt(2, numberOfBeds);
                statement.setLong(3, super.getObjectID());
                statement.executeUpdate();
            }
            return this.getObjectID();
        } else {
            String insertQuery = "INSERT INTO Station (Bezeichnung, Bettenzahl) VALUES (?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(insertQuery,
                    Statement.RETURN_GENERATED_KEYS)) {
                statement.setString(1, name);
                statement.setInt(2, numberOfBeds);
                statement.executeUpdate();
                ResultSet generatedKeys = statement.getGeneratedKeys();
                if (generatedKeys.next()) {
                    setObjectID(generatedKeys.getLong(1));

                    return getObjectID();
                } else {
                    throw new SQLException("Failed to retrieve generated ID.");
                }
            }
        }
    }

    @Override
    public boolean isPersistent() {
        return getObjectID() != PersistentObject.INVALID_OBJECT_ID;
    }

}
