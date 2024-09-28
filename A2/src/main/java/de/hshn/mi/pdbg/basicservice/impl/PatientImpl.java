package de.hshn.mi.pdbg.basicservice.impl;

import de.hshn.mi.pdbg.PersistentObject;
import de.hshn.mi.pdbg.basicservice.BasicDBService;
import de.hshn.mi.pdbg.basicservice.HospitalStay;
import de.hshn.mi.pdbg.basicservice.Patient;
import de.hshn.mi.pdbg.basicservice.jdbc.AbstractPersistentJDBCObject;
import de.hshn.mi.pdbg.exception.FetchException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of the Patient interface. Represents a patient in the medical database service.
 *  Enables database persistence.
 * </p>
 * This class provides methods to set and retrieve patient information such as health insurance details,
 * hospital stays, name, and date of birth.
 * </p>
 *
 * @author Neaga Vlad, Abdul Satar Amiri, Hussein Radujew
 * @version 1.0
 * @see AbstractPersistentJDBCObject
 */
public class PatientImpl extends PersonImpl implements Patient {
    private String healthInsurance;
    private String insuranceNumber;
    private Set<HospitalStay> hospitalStays;


    /**
     * Constructs a new Patient object with the specified database service.
     * The ID contains the default value.
     *
     * @param basicDBService The BasicDBService used for persistence.
     */

    public PatientImpl(BasicDBService basicDBService) {
        super(basicDBService, PersistentObject.INVALID_OBJECT_ID);
        this.hospitalStays = new HashSet<>();
    }

    /**
     * Constructs a new Patient object with the specified parameters.
     *
     * @param service           service containing defined methods in order
     *                          to create persistable objects and also to store,
     *                          fetch and delete such objects from a persistent store.
     * @param id                The unique identifier of the patient.
     * @param healthInsurance   The health insurance of the patient.
     * @param insuranceNumber   The insurance number of the patient.
     * @param lastname          The last name of the patient.
     * @param firstname         The first name of the patient.
     * @param dateOfBirth       The date of birth of the patient.
     */

    public PatientImpl(BasicDBService service, Long id, String healthInsurance, String insuranceNumber, String lastname,
                       String firstname, Date dateOfBirth) {
        super(service, id);
        this.healthInsurance = healthInsurance;
        this.insuranceNumber = insuranceNumber;
        super.setFirstname(firstname);
        super.setLastname(lastname);
        super.setDateOfBirth(dateOfBirth);

        this.hospitalStays = new HashSet<>();
    }

    @Override
    public void setHealthInsurance(String name) {
        this.healthInsurance = name;
    }

    @Override
    public void setInsuranceNumber(String number) {
        this.insuranceNumber = number;
    }

    @Override
    public String getHealthInsurance() {
        return healthInsurance;
    }

    @Override
    public String getInsuranceNumber() {
        return insuranceNumber;
    }

    @Override
    public Set<HospitalStay> getHospitalStays() {
        return hospitalStays;
    }


    @Override
    public void setLastname(String lastname) {
        if (lastname == null || lastname.trim().isEmpty()) {
            throw new AssertionError("Lastname must not be null or empty");
        }
        super.setLastname(lastname);
    }



    @Override
    public void setFirstname(String firstname) {
        if (firstname == null || firstname.trim().isEmpty()) {
            throw new AssertionError("Firstname must not be null or empty");
        }
        super.setFirstname(firstname);
    }



    @Override
    public void setDateOfBirth(Date dateOfBirth) {

        if (dateOfBirth != null && dateOfBirth.after(Date.from(Instant.now())))  {
            throw new AssertionError("The person's birth date cannot be from the future!");
        }
        super.setDateOfBirth(dateOfBirth);
    }

    @Override
    public long getObjectID() {
        return super.getObjectID();
    }

    @Override
    public long store(Connection connection) throws SQLException {

        if (!this.isPersistent()) {
            String sql = "Insert INTO Person (Vorname, Nachname) Values (?, ?);";

            if (!(getDateOfBirth() == null)) {
                sql = "Insert INTO Person (Vorname, Nachname, Geburtsdatum) Values (?, ?, ?);";
            }
            PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, getFirstname());
            statement.setString(2, getLastname());
            if (!(getDateOfBirth() == null)) {
                statement.setDate(3, new java.sql.Date(getDateOfBirth().getTime()));
            }

            statement.executeUpdate();
            ResultSet generatedKeys = statement.getGeneratedKeys();
            long key;
            if (generatedKeys.next()) {
                key = generatedKeys.getLong(1);
            } else  {
                throw new SQLException("Failed to retrieve generated ID.");
            }
            setObjectID(key);
            sql = "Insert INTO Patient (ID, krankenkasse, versicherungsnummer) Values(?, ?, ?)";
            statement = connection.prepareStatement(sql);
            statement.setLong(1, getObjectID());
            statement.setString(2, this.getHealthInsurance());
            statement.setString(3, this.getInsuranceNumber());
            statement.executeUpdate();
            return key;
        } else {

            String sql = "UPDATE Person SET Vorname = ?, Nachname = ?, Geburtsdatum = ? WHERE id = ?;" +
                   "UPDATE Patient SET Krankenkasse = ?, Versicherungsnummer = ? WHERE id = ?;    ";
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setString(1, getFirstname());
            statement.setString(2, getLastname());
            if (getDateOfBirth() != null) {
                statement.setDate(3, new java.sql.Date(getDateOfBirth().getTime()));
            } else {
                statement.setNull(3, java.sql.Types.DATE);
            }
            statement.setLong(4, getObjectID());
            statement.setString(5, getHealthInsurance() == null ? null : getHealthInsurance());
            statement.setString(6, getInsuranceNumber() == null ? null : getInsuranceNumber());
            statement.setLong(7, getObjectID());
            statement.executeUpdate();
            return getObjectID();
        }
    }

    @Override
    public boolean isPersistent() {
        return getObjectID() != PersistentObject.INVALID_OBJECT_ID;
    }
}
