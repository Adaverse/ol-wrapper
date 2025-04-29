# OpenLineage Client Singleton Decorator

This project provides a simple decorator that injects the OpenLineage client into Python functions, along with a utility for creating consistent namespaces.

## Installation

```bash
# Install dependencies
pip install -r requirements.txt
```

## Usage

### OpenLineage Client Decorator

The `with_openlineage_client` decorator injects the OpenLineage client as a parameter to your function:

```python
from src.ol_decorator import with_openlineage_client

# Basic usage - injects ol_client parameter
@with_openlineage_client
def my_function(arg1, arg2, ol_client=None):
    # ol_client is now an instance of OpenLineageClient
    # You can use it to manually emit events
    print(f"Using client: {ol_client}")
    return arg1 + arg2
```

#### Customizing the decorator

You can customize the parameter name and the OpenLineage server URL:

```python
@with_openlineage_client(
    client_param_name="lineage",  # Custom parameter name
    url="http://custom-server:5000"  # Custom OpenLineage server URL
)
def custom_function(x, lineage=None):
    # Use the client with custom parameter name
    return x * x
```

### Namespace Utility

The project includes a utility function to create consistent namespaces:

```python
from src.utils import create_namespace

# Create a namespace based on project and environment
namespace = create_namespace(
    project_name="data_pipeline",    # Required: project name
    environment="prod",              # Required: environment name
    prefix="company",                # Optional: organizational prefix
    include_hostname=True            # Optional: include host name in namespace
)
# Result: 'company.data_pipeline.prod.hostname'
```

### Setting Up Logging

The project includes a utility function to configure logging for the OpenLineage client:

```python
from src.utils import setup_openlineage_logging
import logging

# Set up logging with DEBUG level and log to a file
ol_logger, ol_client_logger = setup_openlineage_logging(
    level=logging.DEBUG,
    log_to_file="openlineage.log"  # Optional: log to a file
)

# Now you can use the loggers
ol_client_logger.debug("This is a debug message")
ol_logger.info("This is an info message")
```

You can also set the logging level using the `OPENLINEAGE_CLIENT_LOGGING` environment variable:

```bash
# Set environment variable before running your script
export OPENLINEAGE_CLIENT_LOGGING=DEBUG
python example.py
```

### Complete Lineage Tracking Example

Using the injected client and namespace utility function to track lineage:

```python
from src.ol_decorator import with_openlineage_client
from src.utils import create_namespace, setup_openlineage_logging
import logging

# Set up logging for OpenLineage
setup_openlineage_logging(level=logging.DEBUG)

@with_openlineage_client
def process_data(data, ol_client=None):
    import uuid
    from datetime import datetime
    from openlineage.client.client import RunEvent
    from openlineage.client.run import Job, Run, RunState
    
    # Generate a unique run ID
    run_id = str(uuid.uuid4())
    
    # Create namespace
    namespace = create_namespace(
        project_name="my_project",
        environment="prod",
        prefix="company"
    )
    
    # Create a simple job name
    job_name = f"process_data.{uuid.uuid4().hex[:8]}"
    
    # Create job and run
    job = Job(namespace=namespace, name=job_name)
    run = Run(runId=run_id)  # Note: parameter is 'runId', not 'id'
    
    # Start event
    start_time = datetime.utcnow().isoformat() + "Z"
    start_event = RunEvent(
        eventType=RunState.START,
        eventTime=start_time,
        run=run,
        job=job,
        producer="my-application",
        inputs=[],
        outputs=[]
    )
    
    # Emit start event
    ol_client.emit(start_event)
    
    # Process data
    result = data * 2
    
    # Complete event
    complete_time = datetime.utcnow().isoformat() + "Z"
    complete_event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=complete_time,
        run=run,
        job=job,
        producer="my-application",
        inputs=[],
        outputs=[]
    )
    
    # Emit complete event
    ol_client.emit(complete_event)
    
    return result
```

### Dataset Example

The project includes an example showing how to create and use datasets in OpenLineage lineage tracking:

```python
from src.ol_decorator import with_openlineage_client
from src.utils import create_namespace, setup_openlineage_logging
from openlineage.client.run import Dataset
from openlineage.client.facet import SchemaDatasetFacet, ColumnLineageDatasetFacet

@with_openlineage_client
def process_with_datasets(input_data, ol_client=None):
    # Create input dataset with schema
    input_dataset = Dataset(
        namespace=namespace,
        name="input_data",
        facets={
            "schema": SchemaDatasetFacet(
                fields=[
                    {"name": "id", "type": "INTEGER"},
                    {"name": "value", "type": "STRING"}
                ]
            )
        }
    )
    
    # Create output dataset with schema and column lineage
    output_dataset = Dataset(
        namespace=namespace,
        name="processed_data",
        facets={
            "schema": SchemaDatasetFacet(
                fields=[
                    {"name": "id", "type": "INTEGER"},
                    {"name": "value", "type": "STRING"},
                    {"name": "processed_at", "type": "TIMESTAMP"}
                ]
            ),
            "columnLineage": ColumnLineageDatasetFacet(
                columnLineage=[
                    {
                        "inputField": {"namespace": namespace, "name": "input_data", "field": "id"},
                        "outputField": {"namespace": namespace, "name": "processed_data", "field": "id"},
                        "transformationDescription": "Direct copy",
                        "transformationType": "IDENTITY"
                    }
                ]
            )
        }
    )
    
    # Emit events with datasets
    start_event = RunEvent(
        eventType=RunState.START,
        eventTime=start_time,
        run=run,
        job=job,
        producer="dataset-example",
        inputs=[input_dataset],
        outputs=[]
    )
    
    # Process data...
    
    complete_event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=complete_time,
        run=run,
        job=job,
        producer="dataset-example",
        inputs=[input_dataset],
        outputs=[output_dataset]
    )
```

This example demonstrates:
- Creating datasets with schema information
- Adding column lineage information
- Including datasets in both START and COMPLETE events
- Tracking data transformations between input and output datasets

## How It Works

- The `with_openlineage_client` decorator uses the `OpenLineageClientSingleton` class to ensure only a single instance of the OpenLineage client exists throughout your application.
- The `create_namespace` function generates a consistent namespace based on your project and environment details.
- The `setup_openlineage_logging` function configures proper logging for the OpenLineage client, enabling you to debug issues.
- The dataset example shows how to track data lineage with detailed schema and column-level information.
