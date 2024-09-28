package de.hshn.mi.pdbg.basicservice.impl;

import de.hshn.mi.pdbg.PersistentObject;
import de.hshn.mi.pdbg.basicservice.BasicDBService;
import de.hshn.mi.pdbg.basicservice.HospitalStay;
import de.hshn.mi.pdbg.basicservice.Patient;
import de.hshn.mi.pdbg.basicservice.Ward;
import de.hshn.mi.pdbg.basicservice.jdbc.AbstractPersistentJDBCObject;
import de.hshn.mi.pdbg.exception.FetchException;

import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
/**
 * Implementation of the HospitalStay interface. This class represents a concrete implementation of a hospital stay
 * that can be stored in the medical database service.
 *
 * @author Neaga Vlad, Abdul Satar Amiri, Hussein Radujew
 * @version 1.0
 */

public class HospitalStayImpl extends AbstractPersistentJDBCObject implements HospitalStay {

    private Date admissionDate;
    private Date dischargeDate;
    private Ward ward;
    private Patient patient;

    /**
     * Creates a new hospital stay with the specified basic database service.
     * The ID contains the default value.
     *
     * @param basicDBService The basic database service.
     */
    public HospitalStayImpl(BasicDBService basicDBService) {
        super(basicDBService, PersistentObject.INVALID_OBJECT_ID);
        setObjectID(PersistentObject.INVALID_OBJECT_ID);
    }

    /**
     * Creates a new hospital stay with the specified parameters.
     *
     * @param basicDBService The service containing defined methods in order
     *                       to create persistable objects and also to store,
     *                       fetch and delete such objects from a persistent store.
     * @param id             The Unique identifier for the hospitalStay-object
     * @param admissionDate  The admission date of the patient.
     * @param dischargeDate  The discharge date of the patient.
     * @param ward           The patient's treatment ward.
     * @param patient        The patient associated with the hospital stay.
     */

    public HospitalStayImpl(BasicDBService basicDBService, long id, Date admissionDate, Date dischargeDate, Ward ward,
                            Patient patient) {
        super(basicDBService, id);
        this.admissionDate = admissionDate;
        this.dischargeDate = dischargeDate;
        this.ward = ward;
        this.patient = patient;
        patient.getHospitalStays().add(this);
    }

    /**
     * Creates a new hospital stay with the specified parameters.
     *
     * @param basicDBService service containing defined methods in order to create persistable objects
     *                       and also to store, fetch and delete such objects from a persistent store.
     * @param admissionDate  The admission date of the patient.
     * @param dischargeDate  The discharge date of the patient.
     * @param ward           The patient's treatment ward.
     * @param patient        The patient associated with the hospital stay.
     */

    public HospitalStayImpl(BasicDBService basicDBService, Date admissionDate, Date dischargeDate, Ward ward,
                            Patient patient) {
        super(basicDBService, PersistentObject.INVALID_OBJECT_ID);
        setObjectID(PersistentObject.INVALID_OBJECT_ID);
        this.admissionDate = admissionDate;
        this.dischargeDate = dischargeDate;
        this.ward = ward;
        this.patient = patient;
        patient.getHospitalStays().add(this);
    }

    @Override
    public Date getAdmissionDate() {
        return admissionDate;
    }

    @Override
    public void setAdmissionDate(Date date) {
        if (date == null) {
            throw new AssertionError("Admission date must not be null");
        }
        // Validate admission date
        if (dischargeDate != null && date.after(dischargeDate)) {
            throw new AssertionError("Admission date must be before discharge date");
        }
        this.admissionDate = date;
    }

    @Override
    public Date getDischargeDate() {
        return dischargeDate;
    }

    @Override
    public void setDischargeDate(Date date) {
        if (date != null && admissionDate != null && date.before(admissionDate)) {
            throw new AssertionError("Discharge date must be after admission date");
        }
        this.dischargeDate = date;
    }

    @Override
    public Ward getWard() {
        if (ward == null) {
            throw new FetchException("Ward should not be null!");
        }
        return ward;
    }

    @Override
    public void setWard(Ward ward) {
        if (ward == null) {
            throw new AssertionError("Ward must not be null");
        }
        this.ward = ward;
    }

    @Override
    public Patient getPatient() {
        if (patient == null) {
            throw new FetchException("Patient should not be null!");
        }

        return patient;
    }

    @Override
    public long getObjectID() {
        return super.getObjectID();
    }

    @Override
    public long store(Connection connection) throws SQLException {
        if (isPersistent()) {
            String updateQuery = "UPDATE Aufenthalt SET Aufnahmedatum = ?, Entlassdatum = ?, SID = ?," +
                    "PID = ? WHERE ID = ?";
            try (PreparedStatement statement = connection.prepareStatement(updateQuery)) {
                statement.setDate(1, new java.sql.Date(admissionDate.getTime()));
                statement.setDate(2, new java.sql.Date(dischargeDate.getTime()));
                statement.setLong(3, ward.getObjectID());
                statement.setLong(4, patient.getObjectID());
                statement.setLong(5, getObjectID());
                statement.executeUpdate();

                return getObjectID();
            }
        } else {
            if (patient.getObjectID() == INVALID_OBJECT_ID) {
                this.getBasicDBService().store(patient);
            }
            if (ward.getObjectID() == INVALID_OBJECT_ID) {
                this.getBasicDBService().store(ward);
            }
            String insertQuery = "INSERT INTO Aufenthalt (Aufnahmedatum, Entlassdatum, SID, PID) VALUES (?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(insertQuery, Statement.RETURN_GENERATED_KEYS)
            ) {
                statement.setDate(1, new java.sql.Date(admissionDate.getTime()));
                if (dischargeDate != null) {
                    statement.setDate(2, new java.sql.Date(dischargeDate.getTime()));
                } else  {
                    statement.setDate(2, null);
                }
                if (!patient.isPersistent()) {
                    this.getBasicDBService().store(patient);
                }
                if (!ward.isPersistent()) {
                    this.getBasicDBService().store(ward);
                }
                statement.setLong(3, ward.getObjectID());
                statement.setLong(4, patient.getObjectID());
                int rowsAffected = statement.executeUpdate();
                if (rowsAffected > 0) {
                    try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                        if (generatedKeys.next()) {
                            long generatedID = generatedKeys.getLong(1);
                            setObjectID(generatedID);
                            return generatedID;
                        }
                    }
                }
                return 0;
            }
        }
    }

    @Override
    public boolean isPersistent() {
        return getObjectID() != PersistentObject.INVALID_OBJECT_ID;
    }
}
