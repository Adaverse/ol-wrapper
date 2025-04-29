# OpenLineage Namespace Utility (Java)

This package provides a Java utility for creating consistent OpenLineage namespaces.

## Usage

The `NamespaceUtils` class provides static methods for creating namespaces:

```java
import com.example.utils.NamespaceUtils;

// Basic usage with required parameters
String namespace = NamespaceUtils.createNamespace(
    "data_pipeline",    // Required: project name
    "prod"             // Required: environment name
);
// Result: "data_pipeline.prod"

// With optional prefix
String namespaceWithPrefix = NamespaceUtils.createNamespace(
    "data_pipeline",    // Required: project name
    "prod",            // Required: environment name
    "company"          // Optional: organizational prefix
);
// Result: "company.data_pipeline.prod"

// With hostname
String namespaceWithHostname = NamespaceUtils.createNamespace(
    "data_pipeline",    // Required: project name
    "prod",            // Required: environment name
    "company",         // Optional: organizational prefix
    true               // Optional: include hostname
);
// Result: "company.data_pipeline.prod.hostname"
```

## Features

- Creates dot-separated namespace strings
- Validates required parameters
- Supports optional organizational prefix
- Optional hostname inclusion
- Convenience methods for common use cases

## Error Handling

The utility throws `IllegalArgumentException` if required parameters are null or empty:

```java
try {
    NamespaceUtils.createNamespace(null, "prod");
} catch (IllegalArgumentException e) {
    System.out.println("Error: " + e.getMessage());
}
```

## Example

See `NamespaceExample.java` for a complete example of how to use the utility. 