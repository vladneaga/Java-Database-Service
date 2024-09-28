package de.hshn.mi.pdbg.basicservice.impl;

import de.hshn.mi.pdbg.PersistentObject;
import de.hshn.mi.pdbg.basicservice.BasicDBService;
import de.hshn.mi.pdbg.basicservice.Person;
import de.hshn.mi.pdbg.basicservice.jdbc.AbstractPersistentJDBCObject;
import java.util.Date;

/**
 * Implementation of the Person interface, representing a person
 * Extends AbstractPersistentJDBCObject for database persistence.
 * <p>
 * This class provides methods to set and retrieve person information such as name and date of birth.
 * </p>
 *
 * @author Neaga Vlad, Abdul Satar Amiri, Hussein Radujew
 * @version 1.0
 */
public abstract class PersonImpl extends AbstractPersistentJDBCObject implements Person {
    private String lastname;
    private String firstname;
    private Date dateOfBirth;

    /**
     * Constructs a new Person-object with the specified BasicDBService.
     * The ID contains the default value.
     *
     * @param basicDBService service for creating persistable objects and storing, fetching, and deleting such
     *                       objects from a persistent store.
     */
    public PersonImpl(BasicDBService basicDBService) {
        super(basicDBService, PersistentObject.INVALID_OBJECT_ID);
    }

    /**
     * Constructs a new person-object with the specified BasicDBService and ID.
     *
     * @param basicDBService service for creating persistable objects and storing, fetching, and deleting such
     *                       objects from a persistent store.
     * @param id             the unique identifier of a person
     */
    public PersonImpl(BasicDBService basicDBService, Long id) {
        super(basicDBService, id);
    }

    @Override
    public String getLastname() {
        return lastname;
    }

    @Override
    public void setLastname(String lastname) {
        if (lastname == null || lastname.trim().isEmpty()) {
            throw new AssertionError("Lastname must not be null or empty");
        }
        this.lastname = lastname.trim();
    }

    @Override
    public String getFirstname() {
        return firstname;
    }

    @Override
    public void setFirstname(String firstname) {
        if (firstname == null || firstname.trim().isEmpty()) {
            throw new AssertionError("Firstname must not be null or empty");
        }
        this.firstname = firstname.trim();
    }

    @Override
    public Date getDateOfBirth() {
        return dateOfBirth;
    }

    @Override
    public void setDateOfBirth(Date dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

}