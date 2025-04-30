package com.ol.OlExample;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.Job;
import io.openlineage.client.OpenLineage.JobFacets;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.Run;
import io.openlineage.client.OpenLineage.RunFacets;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.utils.UUIDUtils;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;

import java.net.URI;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.ol.OlClient.OlClient;

public class DatasetExample {
    public static void emitRunEvent(
        String namespace
    ) {
        URI producer = URI.create("producer");
        OpenLineage ol = new OpenLineage(producer);
        List<SchemaDatasetFacetFields> fields = Arrays.asList(
            ol.newSchemaDatasetFacetFields("id", "INTEGER", null, null)
        );
        List<InputDataset> inputs =
        Arrays.asList(
            ol.newInputDatasetBuilder()
                .namespace("ins")
                .name("input")
                .facets(
                    ol.newDatasetFacetsBuilder()
                        .version(ol.newDatasetVersionDatasetFacet("input-version"))
                        .schema(ol.newSchemaDatasetFacet(Arrays.asList()))
                        .build())
                .inputFacets(
                    ol.newInputDatasetInputFacetsBuilder()
                        .
                        .build())
                .build());
    };
}
