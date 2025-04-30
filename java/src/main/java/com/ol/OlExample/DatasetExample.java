package com.ol.OlExample;

import java.net.URI;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.ol.OlClient.OlClient;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.Job;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.Run;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.utils.UUIDUtils;

public class DatasetExample {
    public static void emitRunEvent(
        String namespace
    ) {
        OpenLineageClient olClient = OlClient.getOlClient();
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
        URI producer = URI.create("producer");
        OpenLineage ol = new OpenLineage(producer);

        List<SchemaDatasetFacetFields> inputFields = Arrays.asList(
            ol.newSchemaDatasetFacetFields("id", "INTEGER", null, null),
            ol.newSchemaDatasetFacetFields("value", "STRING", null, null)
        );
        List<SchemaDatasetFacetFields> outputFields = Arrays.asList(
            ol.newSchemaDatasetFacetFields("id", "INTEGER", null, null),
            ol.newSchemaDatasetFacetFields("name", "STRING", null, null),
            ol.newSchemaDatasetFacetFields("processed_at", "TIMESTAMP", null, null)
        );

        List<InputDataset> inputs =
        Arrays.asList(
            ol.newInputDatasetBuilder()
                .namespace("ins")
                .name("input")
                .facets(
                    ol.newDatasetFacetsBuilder()
                        .version(ol.newDatasetVersionDatasetFacet("input-version"))
                        .schema(ol.newSchemaDatasetFacet(inputFields))
                        .build())
                .build());

        List<OutputDataset> outputs = 
        Arrays.asList(
            ol.newOutputDatasetBuilder()
                .namespace(namespace)
                .name("output")
                .facets(
                    ol.newDatasetFacetsBuilder()
                        .version(ol.newDatasetVersionDatasetFacet("output-version"))
                        .schema(ol.newSchemaDatasetFacet(outputFields))
                        .build()
                )
                .build()
        );
        Job job = ol.newJobBuilder().namespace(namespace).name("testJob").build();
        UUID runId = UUIDUtils.generateNewUUID();
        Run run = ol.newRunBuilder().runId(runId).build();

        // run state update which encapsulates all - with START event in this case
        RunEvent runStateUpdate =
        ol.newRunEventBuilder()
            .eventType(OpenLineage.RunEvent.EventType.START)
            .eventTime(now)
            .run(run)
            .job(job)
            .inputs(inputs)
            .outputs(outputs)
            .build();
        olClient.emit(runStateUpdate);
    };
}
