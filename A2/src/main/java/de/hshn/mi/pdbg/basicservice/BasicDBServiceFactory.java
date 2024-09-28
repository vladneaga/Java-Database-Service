package de.hshn.mi.pdbg.basicservice;


import de.hshn.mi.pdbg.basicservice.services.BasicDBServiceImpl;

/**
 * {@link BasicDBServiceFactory} define a static factory method in order to create an instance of a
 * {@link BasicDBService} object.
 * @ version 1.0
 */
public class BasicDBServiceFactory {
    /**
* Factory method in order to create a instance of a {@link BasicDBService} object.
* @ return instance of a {@link BasicDBService} object
* */
    public static BasicDBService createBasicDBService() {
        //return new BasicDBServiceImpl("jdbc:postgresql://postgres/pdbg-a2", "postgres", "postgres");
        return new BasicDBServiceImpl("jdbc:postgresql://postgres/pdbg-a2", "postgres", "postgres");
        //The code needed to instantiate an implementation of a BasicDBService.
        // Use "jdbc:postgresql://postgres/pdbg-a2", "postgres", "postgres" for jdbc url,
        // login and password in CI/CD context
        //throw new UnsupportedOperationException();
        // zeichensatze
    }
}
