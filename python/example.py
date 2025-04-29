"""
Example of using the OpenLineage client singleton decorator.
"""

import logging
from src.ol_decorator import with_openlineage_client
from src.utils import create_namespace, setup_openlineage_logging

# Configure basic logging
logging.basicConfig(level=logging.INFO)

# Set up OpenLineage client logging (this will enable DEBUG logs for OpenLineage)
ol_logger, ol_client_logger = setup_openlineage_logging(
    level=logging.DEBUG,
    log_to_file="openlineage.log"  # Optional: log to a file
)


@with_openlineage_client
def function_with_client(a, b, ol_client=None):
    """
    A function that receives the OpenLineage client directly.
    You can use the client to manually emit events.
    """
    logging.info(f"Using OpenLineage client: {ol_client}")
    
    # You can manually emit events if needed
    # ol_client.emit(...)
    
    return a * b


@with_openlineage_client(client_param_name="lineage_client", url="http://custom-server:5000")
def custom_client_param(x, lineage_client=None):
    """
    Example showing how to customize the client parameter name and URL.
    """
    logging.info(f"Using custom-named client parameter: {lineage_client}")
    return x * x


def manual_lineage_tracking(a, b, prefix=None, environment="dev"):
    """
    Example showing how to manually track lineage using the injected client
    and the namespace utility function.
    """
    @with_openlineage_client
    def internal_function(a, b, ol_client=None):
        import uuid
        from datetime import datetime
        from openlineage.client.client import RunEvent
        from openlineage.client.run import Job, Run, RunState
        
        # Log the client instance
        ol_client_logger.debug(f"OpenLineage client instance: {ol_client}")
        
        # Generate a unique run ID
        run_id = str(uuid.uuid4())
        ol_client_logger.debug(f"Generated run ID: {run_id}")
        
        # Use utility function to create namespace
        namespace = create_namespace(
            project_name="ol_poc",
            environment=environment,
            prefix=prefix,
            include_hostname=True
        )
        
        # Create a simple job name
        job_name = f"internal_function.a{a}_b{b}"
        
        logging.info(f"Using namespace: {namespace}")
        logging.info(f"Using job name: {job_name}")
        
        # Create a job and run
        job = Job(namespace=namespace, name=job_name)
        run = Run(runId=run_id)
        
        # Start event
        start_time = datetime.utcnow().isoformat() + "Z"
        ol_client_logger.debug(f"Creating START event at {start_time}")
        
        start_event = RunEvent(
            eventType=RunState.START,
            eventTime=start_time,
            run=run,
            job=job,
            producer="manual-example",
            inputs=[],
            outputs=[]
        )
        
        # Emit start event
        ol_client_logger.debug("Emitting START event")
        ol_client.emit(start_event)
        
        # Do the work
        ol_client_logger.debug(f"Performing operation: {a} * {b}")
        result = a * b
        
        # Complete event
        complete_time = datetime.utcnow().isoformat() + "Z"
        ol_client_logger.debug(f"Creating COMPLETE event at {complete_time}")
        
        complete_event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=complete_time,
            run=run,
            job=job,
            producer="manual-example",
            inputs=[],
            outputs=[]
        )
        
        # Emit complete event
        ol_client_logger.debug("Emitting COMPLETE event")
        ol_client.emit(complete_event)
        
        return result
    
    return internal_function(a, b)

@with_openlineage_client
def test_ol_ds(ol_client=None):
    from openlineage.client.run import Dataset, DatasetEvent
    from datetime import datetime
    
    namespace = create_namespace(
        project_name="ol_poc",
        environment="uat",
        include_hostname=False
    )
    ds = Dataset(namespace=namespace, name="first_dataset", facets=[])
    dse = DatasetEvent(dataset=ds, producer="sample_producer", eventTime=datetime.utcnow().isoformat() + "Z", schemaURL="https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent")
    ol_client.emit(dse)  

def dataset_example(input_data, prefix=None, environment="dev"):
    """
    Example showing how to create and use datasets in OpenLineage lineage tracking.
    """
    @with_openlineage_client
    def process_with_datasets(input_data, ol_client=None):
        import uuid
        from datetime import datetime
        from openlineage.client.client import RunEvent
        from openlineage.client.run import Job, Run, RunState
        from openlineage.client.facet import SchemaDatasetFacet, ColumnLineageDatasetFacet, DatasetVersionDatasetFacet
        from openlineage.client.run import Dataset
        
        # Generate a unique run ID
        run_id = str(uuid.uuid4())
        ol_client_logger.debug(f"Generated run ID: {run_id}")
        
        # Create namespace
        namespace = create_namespace(
            project_name="ol_poc",
            environment=environment,
            prefix=prefix,
            include_hostname=True
        )
        
        # Create job name
        job_name = "process_with_datasets"
        
        # Create input dataset
        input_dataset = Dataset(
            namespace=namespace,
            name="input_data",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        {"name": "id", "type": "INTEGER"},
                        {"name": "value", "type": "STRING"},
                        {
                            "name": "example_stuct", 
                            "type": "struct",
                            "description": "it example struct",
                            "fields": [
                                {"name": "val1", "type": "STRING"},
                                {"name": "val2", "type": "STRING"},
                            ]
                        }
                    ]
                ),
                "version": DatasetVersionDatasetFacet(datasetVersion=2)
            }
        )
        
        # Create output dataset
        output_dataset = Dataset(
            namespace=namespace,
            name="processed_data",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        {"name": "id", "type": "INTEGER"},
                        {"name": "name", "type": "STRING"},
                        {"name": "processed_at", "type": "TIMESTAMP"}
                    ]
                ),
                "columnLineage": ColumnLineageDatasetFacet(
                    fields={
                        "id": {
                            "inputFields": [{
                                "namespace": namespace,
                                "name": "input_data",
                                "field": "id",
                            }]
                        },
                        "name": {
                            "inputFields": [{
                                "namespace": namespace,
                                "name": "input_data",
                                "field": "value",
                            }]
                        }
                    }
                ),
                "version": DatasetVersionDatasetFacet(datasetVersion=2)
            }
        )
        
        # Create job and run
        job = Job(namespace=namespace, name=job_name)
        run = Run(runId=run_id)
        
        # Start event with input dataset
        start_time = datetime.utcnow().isoformat() + "Z"
        ol_client_logger.debug(f"Creating START event at {start_time}")
        
        start_event = RunEvent(
            eventType=RunState.START,
            eventTime=start_time,
            run=run,
            job=job,
            producer="dataset-example",
            inputs=[input_dataset],
            outputs=[]
        )
        
        # Emit start event
        ol_client_logger.debug("Emitting START event")
        ol_client.emit(start_event)
        
        # Process the data
        ol_client_logger.debug("Processing input data")
        processed_data = [
            {**item, "processed_at": datetime.utcnow().isoformat()}
            for item in input_data
        ]
        
        # Complete event with both input and output datasets
        complete_time = datetime.utcnow().isoformat() + "Z"
        ol_client_logger.debug(f"Creating COMPLETE event at {complete_time}")
        
        complete_event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=complete_time,
            run=run,
            job=job,
            producer="dataset-example",
            inputs=[input_dataset],
            outputs=[output_dataset]
        )
        
        # Emit complete event
        ol_client_logger.debug("Emitting COMPLETE event")
        ol_client.emit(complete_event)
        
        return processed_data
    
    return process_with_datasets(input_data)


if __name__ == "__main__":
    logging.info("Starting OpenLineage client example...")
    
    # Client injection examples
    # logging.info("=== Client Injection Examples ===")
    # result1 = function_with_client(4, 7)
    # logging.info(f"Function with client result: {result1}")
    
    # result2 = custom_client_param(5)
    # logging.info(f"Custom client param result: {result2}")
    
    # # Example with default namespace
    # logging.info("\n=== Default Namespace Example ===")
    # result3 = manual_lineage_tracking(3, 6)
    # logging.info(f"Manual lineage tracking result: {result3}")
    
    # # Example with custom namespace
    # logging.info("\n=== Custom Namespace Example ===")
    # result4 = manual_lineage_tracking(5, 8, prefix="company", environment="staging")
    # logging.info(f"Custom namespace lineage tracking result: {result4}")
    
    # Dataset example
    logging.info("\n=== Dataset Example ===")
    sample_data = [
        {"id": 1, "value": "first"},
        {"id": 2, "value": "second"}
    ]
    result5 = dataset_example(sample_data, prefix="company", environment="prod")
    logging.info(f"Dataset processing result: {result5}")

    
    