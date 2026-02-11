package com.assn.tcap.rest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main entry point for the Ingestor application.
 */
@SpringBootApplication
public class RestApp {

    /**
     * Protected constructor.
     */
    protected RestApp() {
    }

    /**
     * Main method to start the application.
     *
     * @param args command line arguments
     */
    public static void main(final String[] args) {
        SpringApplication.run(RestApp.class, args);
    }
}
