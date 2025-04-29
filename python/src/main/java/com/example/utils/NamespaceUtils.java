package com.example.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Utility class for creating OpenLineage namespaces.
 */
public class NamespaceUtils {
    
    /**
     * Creates a namespace string based on project and environment details.
     *
     * @param projectName The name of the project (required)
     * @param environment The environment name (required)
     * @param prefix Optional organizational prefix
     * @param includeHostname Whether to include the hostname in the namespace
     * @return A dot-separated namespace string
     * @throws IllegalArgumentException if required parameters are null or empty
     */
    public static String createNamespace(
            String projectName,
            String environment,
            String prefix,
            boolean includeHostname) {
        
        // Validate required parameters
        if (projectName == null || projectName.trim().isEmpty()) {
            throw new IllegalArgumentException("projectName is required");
        }
        if (environment == null || environment.trim().isEmpty()) {
            throw new IllegalArgumentException("environment is required");
        }
        
        List<String> parts = new ArrayList<>();
        
        // Add prefix if provided
        if (prefix != null && !prefix.trim().isEmpty()) {
            parts.add(prefix.trim());
        }
        
        // Add project name and environment
        parts.add(projectName.trim());
        parts.add(environment.trim());
        
        // Add hostname if requested
        if (includeHostname) {
            try {
                String hostname = InetAddress.getLocalHost().getHostName();
                parts.add(hostname);
            } catch (UnknownHostException e) {
                // Log warning but continue without hostname
                System.err.println("Warning: Could not get hostname: " + e.getMessage());
            }
        }
        
        // Join all parts with dots
        return String.join(".", parts);
    }
    
    /**
     * Creates a namespace string based on project and environment details.
     * This is a convenience method that doesn't include the hostname.
     *
     * @param projectName The name of the project (required)
     * @param environment The environment name (required)
     * @param prefix Optional organizational prefix
     * @return A dot-separated namespace string
     * @throws IllegalArgumentException if required parameters are null or empty
     */
    public static String createNamespace(
            String projectName,
            String environment,
            String prefix) {
        return createNamespace(projectName, environment, prefix, false);
    }
    
    /**
     * Creates a namespace string based on project and environment details.
     * This is a convenience method that doesn't include a prefix or hostname.
     *
     * @param projectName The name of the project (required)
     * @param environment The environment name (required)
     * @return A dot-separated namespace string
     * @throws IllegalArgumentException if required parameters are null or empty
     */
    public static String createNamespace(
            String projectName,
            String environment) {
        return createNamespace(projectName, environment, null, false);
    }
} 