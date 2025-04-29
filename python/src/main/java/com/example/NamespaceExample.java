package com.example;

import com.example.utils.NamespaceUtils;

/**
 * Example class demonstrating how to use the NamespaceUtils.
 */
public class NamespaceExample {
    
    public static void main(String[] args) {
        // Example 1: Basic usage with required parameters
        String namespace1 = NamespaceUtils.createNamespace(
            "data_pipeline",
            "prod"
        );
        System.out.println("Basic namespace: " + namespace1);
        
        // Example 2: With prefix
        String namespace2 = NamespaceUtils.createNamespace(
            "data_pipeline",
            "prod",
            "company"
        );
        System.out.println("Namespace with prefix: " + namespace2);
        
        // Example 3: With hostname
        String namespace3 = NamespaceUtils.createNamespace(
            "data_pipeline",
            "prod",
            "company",
            true
        );
        System.out.println("Namespace with hostname: " + namespace3);
        
        // Example 4: Error handling
        try {
            NamespaceUtils.createNamespace(null, "prod");
        } catch (IllegalArgumentException e) {
            System.out.println("Error caught: " + e.getMessage());
        }
    }
} 