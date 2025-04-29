package com.ol.OlClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class CreateNamespace {

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

}
