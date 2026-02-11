package com.assn.tcap.ingestor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main entry point for the Ingestor application.
 */
@SpringBootApplication
public class IngestorApp {

    /**
     * Protected constructor.
     */
    protected IngestorApp() {
    }

    /**
     * Main method to start the application.
     *
     * @param args command line arguments
     */
    public static void main(final String[] args) {
        SpringApplication.run(IngestorApp.class, args);
    }
}
