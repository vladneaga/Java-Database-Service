package de.hshn.mi.pdbg.basicservice.services;

import de.hshn.mi.pdbg.PersistentObject;
import de.hshn.mi.pdbg.basicservice.BasicDBService;
import de.hshn.mi.pdbg.basicservice.impl.HospitalStayImpl;
import de.hshn.mi.pdbg.basicservice.impl.PatientImpl;

import de.hshn.mi.pdbg.basicservice.impl.WardImpl;
import de.hshn.mi.pdbg.basicservice.HospitalStay;
import de.hshn.mi.pdbg.basicservice.Patient;
import de.hshn.mi.pdbg.basicservice.Ward;
import de.hshn.mi.pdbg.exception.FetchException;
import de.hshn.mi.pdbg.exception.StoreException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
/**
 * Implementation of the BasicDBService interface. Provides methods to interact with the medical database service.
 * Manages the creation, retrieval, and removal of patients, wards, and hospital stays.
 * Implements methods for database storage and retrieval.
 *
 * @author Neaga Vlad, Abdul Satar Amiri
 * @version 1.0
 */

public class BasicDBServiceImpl implements BasicDBService {
    private String dbUrl;
    private  String user;
    private String pass;
    private Connection conn;

    public String getDB_URL() {
        return dbUrl;
    }

    public String getUSER() {
        return user;
    }

    public String getPASS() {
        return pass;
    }

    /**
     * Constructs a database service with the specified database URL, username, and password.
     *
     * @param dbUrl The connection-String of the database.
     * @param user   The login credentials for accessing the database.
     * @param pass   The password for accessing the database.
     */

    public BasicDBServiceImpl(String dbUrl, String user, String pass) {
        this.dbUrl = dbUrl;
        this.user = user;
        this.pass = pass;
        try {
            conn = DriverManager.getConnection(dbUrl, user, pass);
        } catch (SQLException e) {
            throw new FetchException(e);
        }
    }

    public Connection getConnection() {
        return conn;
    }

    @Override
    public Patient createPatient(String nachname, String vorname) {
        Patient patient = new PatientImpl(this);
        patient.setFirstname(vorname);
        patient.setLastname(nachname);

        return patient;
    }

    @Override
    public Ward createWard(String bezeichnung, int bettenzahl) {
        Ward ward = new WardImpl(this);
        ward.setName(bezeichnung);
        ward.setNumberOfBeds(bettenzahl);

        return ward;
    }

    @Override
    public HospitalStay createHospitalStay(Patient patient, Ward ward, Date date) {
        if (date == null || patient == null || ward == null) {
            throw new AssertionError("Null parameter");
        }
        return new HospitalStayImpl(this, date, null, ward, patient);
    }

    @Override
    public void removeHospitalStay(long id) {
        restoreConnection();
        if (id <= 0 || id == PersistentObject.INVALID_OBJECT_ID) {
            throw new AssertionError("The Hospital Stay ID should be greate than 0");
        }
        String sqlDelete = "DELETE FROM Aufenthalt WHERE ID = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sqlDelete)) {
            pstmt.setLong(1, id);

            int rowsDeleted = pstmt.executeUpdate();

            if (rowsDeleted <= 0) {
                throw new StoreException("Hospital Stay with id " + id + " does not exist");
            }
        } catch (SQLException e) {
            throw new FetchException(e);
        }
    }

    @Override
    public List<Patient> getPatients(String lastname, String firstname, Date startDate, Date endDate) {
        restoreConnection();
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        List<Patient> patients = new ArrayList<>();

        try {
            StringBuilder queryBuilder = new StringBuilder("SELECT p.ID, Vorname, Nachname, Geburtsdatum, Krankenkasse,"
                    + "Versicherungsnummer FROM Patient p ");
            queryBuilder.append("JOIN Person pe ON p.ID = pe.ID WHERE 1=1");

            if (lastname != null) {
                queryBuilder.append(" AND Nachname LIKE ?");
            }
            if (firstname != null) {
                queryBuilder.append(" AND Vorname LIKE ?");
            }
            if (startDate != null) {
                queryBuilder.append(" AND Geburtsdatum >= ?");
            }
            if (endDate != null) {
                queryBuilder.append(" AND Geburtsdatum <= ?");
            }

            pstmt = getConnection().prepareStatement(queryBuilder.toString());

            int paramIndex = 1;
            if (lastname != null) {
                pstmt.setString(paramIndex++, /*"%" +*/ lastname /*+ "%"*/);
            }
            if (firstname != null) {
                pstmt.setString(paramIndex++, /*"%" +*/ firstname /*+ "%"*/);
            }
            if (startDate != null) {
                pstmt.setDate(paramIndex++, new java.sql.Date(startDate.getTime()));
            }
            if (endDate != null) {
                pstmt.setDate(paramIndex++, new java.sql.Date(endDate.getTime()));
            }

            resultSet = pstmt.executeQuery();

            while (resultSet.next()) {
                long patientID = resultSet.getLong("ID");
                String firstName = resultSet.getString("Vorname");
                String lastName = resultSet.getString("Nachname");
                Date dateOfBirth = resultSet.getDate("Geburtsdatum");
                String healthInsurance = resultSet.getString("Krankenkasse");
                String insuranceNumber = resultSet.getString("Versicherungsnummer");
                Patient patient = new PatientImpl(this, patientID, healthInsurance, insuranceNumber, lastName,
                        firstName, dateOfBirth);

                patients.add(patient);
            }
        } catch (SQLException e) {
            throw new FetchException(e);
        }

        return patients;
    }

    @Override
        public Patient getPatient(long patientID) {
        restoreConnection();
        if (patientID <= 0 || patientID == PersistentObject.INVALID_OBJECT_ID) {
            throw new AssertionError("The Patient ID should be greate than 0");
        }
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        Patient patient = null;

        try {
            String query = "SELECT p.ID, Vorname, Nachname, Geburtsdatum, Krankenkasse," +
                    "Versicherungsnummer FROM Patient p " + "JOIN Person pe ON p.ID = pe.ID WHERE p.ID = ?";

            pstmt = getConnection().prepareStatement(query);
            pstmt.setLong(1, patientID);
            resultSet = pstmt.executeQuery();

            if (resultSet.next()) {
                String firstName = resultSet.getString("Vorname");
                String lastName = resultSet.getString("Nachname");
                Date dateOfBirth = resultSet.getDate("Geburtsdatum");
                String healthInsurance = resultSet.getString("Krankenkasse");
                String insuranceNumber = resultSet.getString("Versicherungsnummer");
                patient = new PatientImpl(this, patientID, healthInsurance, insuranceNumber,
                        lastName, firstName, dateOfBirth);

            }
        } catch (SQLException e) {
            throw new FetchException(e);
        }
        return patient;
    }

    @Override
    public List<Ward> getWards() {
        restoreConnection();
        try {
            if (getConnection().isClosed()) {
                this.conn = DriverManager.getConnection(getDB_URL(), getUSER(), getPASS());
            }
        } catch (SQLException e) {
            throw new FetchException(e);
        }
        List<Ward> wards = new ArrayList<>();
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;

        try {
            connection = getConnection();
            String query = "SELECT ID, Bezeichnung, Bettenzahl FROM Station";

            pstmt = connection.prepareStatement(query);
            resultSet = pstmt.executeQuery();

            while (resultSet.next()) {
                long wardID = resultSet.getLong("ID");
                String name = resultSet.getString("Bezeichnung");
                int numberOfBeds = resultSet.getInt("Bettenzahl");

                Ward ward = new WardImpl(this, wardID, numberOfBeds, name);
                wards.add(ward);
            }
        } catch (SQLException e) {
            throw new FetchException(e);
        }

        return wards;
    }

    @Override
    public Ward getWard(long wardID) {
        restoreConnection();
        if (wardID <= 0 || wardID == PersistentObject.INVALID_OBJECT_ID) {
            throw new AssertionError("The Ward ID should be greate than 0");
        }
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        Ward ward = null;

        try {
            connection = getConnection();
            String query = "SELECT Bezeichnung, Bettenzahl FROM Station WHERE ID = ?";
            pstmt = connection.prepareStatement(query);
            pstmt.setLong(1, wardID);
            resultSet = pstmt.executeQuery();

            if (resultSet.next()) {
                String name = resultSet.getString("Bezeichnung");
                int numberOfBeds = resultSet.getInt("Bettenzahl");
                ward = new WardImpl(this, wardID, numberOfBeds, name);
            }
        } catch (SQLException e) {
            throw new FetchException(e);
        }
        return ward;
    }

    @Override
    public List<HospitalStay> getHospitalStays(long patientID) {
        restoreConnection();
        if (patientID <= 0 || patientID == PersistentObject.INVALID_OBJECT_ID) {
            throw new AssertionError("The patient ID should be greate than 0");
        }

        List<HospitalStay> hospitalStays = new ArrayList<>();
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;

        try {
            connection = getConnection();

            String query = "SELECT ID, PID, SID, Aufnahmedatum, Entlassdatum FROM Aufenthalt WHERE PID = ?";

            pstmt = connection.prepareStatement(query);
            pstmt.setLong(1, patientID);

            resultSet = pstmt.executeQuery();

            while (resultSet.next()) {
                long stayID = resultSet.getLong("ID");
                long wardID = resultSet.getLong("SID");
                long patientIDFromQuery = resultSet.getLong("PID");
                Date admissionDate = resultSet.getDate("Aufnahmedatum");
                Date dischargeDate = resultSet.getDate("Entlassdatum");
                HospitalStay hospitalStay = new HospitalStayImpl(this, stayID, admissionDate,
                        dischargeDate, getWard(wardID), getPatient(patientIDFromQuery));
                hospitalStays.add(hospitalStay);
            }
        } catch (SQLException e) {
            throw new FetchException(e);
        }

        return hospitalStays;
    }
    /**
     * Checks if a patient exists with the given ID.
     *
     * @param patientID the ID of the patient to check
     * @return true if the patient exists, false otherwise
     * @throws SQLException if an SQL exception occurs
     */

    @Override
    public List<HospitalStay> getHospitalStays(long patientID, Date startDate, Date endDate) {
        restoreConnection();
        if (patientID <= 0 || patientID == PersistentObject.INVALID_OBJECT_ID) {
            throw new AssertionError("The patient ID should be greate than 0");
        }
        if (endDate != null && startDate != null && startDate.after(endDate)) {
            throw new AssertionError("The start date cannot be later than the enddate!");
        }
        if (patientID <= 0 || patientID == PersistentObject.INVALID_OBJECT_ID) {
            throw new AssertionError("The patient ID should be greate than 0");
        }

        List<HospitalStay> hospitalStays = new ArrayList<>();
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;

        try {
            connection = getConnection();
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("SELECT ID, PID, SID, Aufnahmedatum, Entlassdatum FROM Aufenthalt WHERE PID = ?");
            if (startDate != null) {
                queryBuilder.append(" AND Aufnahmedatum >= ?");
            }
            if (endDate != null) {
                queryBuilder.append(" AND Entlassdatum <= ?");
            }

            pstmt = connection.prepareStatement(queryBuilder.toString());
            pstmt.setLong(1, patientID);

            int parameterIndex = 2;
            if (startDate != null) {
                pstmt.setDate(parameterIndex++, new java.sql.Date(startDate.getTime()));
            }
            if (endDate != null) {
                pstmt.setDate(parameterIndex++, new java.sql.Date(endDate.getTime()));
            }

            resultSet = pstmt.executeQuery();

            while (resultSet.next()) {
                long stayID = resultSet.getLong("ID");
                long wardID = resultSet.getLong("SID");
                Date admissionDate = resultSet.getDate("Aufnahmedatum");
                Date dischargeDate = resultSet.getDate("Entlassdatum");

                HospitalStay hospitalStay = new HospitalStayImpl(this, stayID, admissionDate,
                        dischargeDate, getWard(wardID), getPatient(patientID));
                hospitalStays.add(hospitalStay);
            }
        } catch (SQLException e) {
            throw new FetchException(e);
        }

        return hospitalStays;
    }

    /**
     * Restores the database connection if it is closed.
     * If the connection is closed, it establishes a new connection using the stored database URL,
     * username, and password.
     * This method ensures that the database connection is available for use and prevents
     * SQLExceptions due to closed connections.
     *
     * @return The restored database connection.
     * @throws RuntimeException If an SQL exception occurs while restoring the connection.
     */
    public Connection restoreConnection() {
        try {
            if (getConnection().isClosed()) {
                this.conn = DriverManager.getConnection(getDB_URL(), getUSER(), getPASS());
            }
        } catch (SQLException e) {
            throw new FetchException(e);
        }
        return this.conn;
    }



    @Override
    public double getAverageHospitalStayDuration(long wardID) {
        restoreConnection();
        if (wardID <= 0 || wardID == PersistentObject.INVALID_OBJECT_ID) {
            throw new AssertionError("Invalid ward ID");
        }
        double averageStayDuration = 0.0;
        try (Connection connection = getConnection()) {
            String query = "SELECT AVG(Entlassdatum - Aufnahmedatum) AS avg_duration " +
                    "FROM Aufenthalt " +
                    "WHERE SID = ? AND Aufnahmedatum IS NOT NULL AND Entlassdatum IS NOT NULL";

            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setLong(1, wardID);

                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        averageStayDuration = resultSet.getDouble("avg_duration");
                    }
                }
            }
        } catch (SQLException e) {

            throw new FetchException(e);
        }
        return averageStayDuration;
    }

    @Override
    public int getAllocatedBeds(Ward ward) {
        restoreConnection();
        if (ward != null && !ward.isPersistent()) {
            throw new AssertionError("The specified ward must be persistent");
        }

        int allocatedBedsCount = 0;
        try (Connection connection = getConnection()) {
            String query;
            if (ward != null) {
                query = "SELECT COUNT(*) AS allocated_beds " +
                        "FROM Aufenthalt " +
                        "WHERE SID = ? AND Entlassdatum IS NULL";
            } else {
                query = "SELECT COUNT(*) AS allocated_beds " +
                        "FROM Aufenthalt " +
                        "WHERE Entlassdatum IS NULL";
            }
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                if (ward != null) {
                    statement.setLong(1, ward.getObjectID());
                }
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        allocatedBedsCount = resultSet.getInt("allocated_beds");
                    }
                }
            }
        } catch (SQLException e) {
            throw new FetchException(e);
        }

        return allocatedBedsCount;
    }
    //CONSIDER using one function in another

    /**
     * Retrieves the number of free beds in the specified ward or in all wards if no ward is specified.
     * Calculates the number of free beds by subtracting the count of occupied beds from the total bed count
     * in the ward(s).
     *
     * @param ward The ward for which to retrieve the number of free beds. If null, retrieves
     *        the number of free beds in all wards.
     * @return The number of free beds in the specified ward or in all wards if no ward is specified.
     * @throws AssertionError If the specified ward is not persistent.
     * @throws RuntimeException If an SQL exception occurs while querying the database.
     */
    public int getFreeBeds(Ward ward) {
        restoreConnection();
        if (ward != null && !ward.isPersistent()) {
            throw new AssertionError("The specified ward must be persistent");
        }

        int freeBedsCount = 0;

        try (Connection connection = getConnection()) {
            String query;
            if (ward != null) {
                query = "SELECT Bettenzahl - (\n" +
                    "    SELECT COUNT(*) \n" +
                    "    FROM Aufenthalt \n" +
                    "    WHERE Aufenthalt.SID = Station.ID AND Aufenthalt.Entlassdatum IS NULL\n" +
                    ") AS free_beds\n" +
                    "FROM Station\n" +
                    "WHERE Station.ID = ?;";
            } else {
                query = "SELECT SUM(Bettenzahl - (\n" +
                    "    SELECT COUNT(*)\n" +
                    "    FROM Aufenthalt\n" +
                    "    WHERE Aufenthalt.SID = Station.ID AND Aufenthalt.Entlassdatum IS NULL\n" +
                    ")) AS free_beds\n" +
                    "FROM Station;";;
            }
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                if (ward != null) {
                    statement.setLong(1, ward.getObjectID());
                }
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        freeBedsCount = resultSet.getInt("free_beds");
                    }
                }
            }
        } catch (SQLException e) {
            throw new FetchException(e);
        }

        return freeBedsCount;
    }

    @Override
   public long store(PersistentObject persistentObject) {
        if (persistentObject == null) {
            throw new AssertionError("The object should not be null!");
        }
        restoreConnection();
        try {
            if (persistentObject instanceof Patient) {
                return ((PatientImpl) persistentObject).store(getConnection());
            } else if (persistentObject instanceof Ward) {
                return ((WardImpl) persistentObject).store(getConnection());
            } else if (persistentObject instanceof HospitalStay) {
                return ((HospitalStayImpl) persistentObject).store(getConnection());
            } else {
                throw new StoreException("Unsupported type: " + persistentObject.getClass().getName());
            }
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }


    @Override
    public void close() {
        try {
            this.conn.close();
        } catch (SQLException e) {
            throw new FetchException(e);
        }
    }






}
