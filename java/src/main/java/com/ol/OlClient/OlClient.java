package com.ol.OlClient;

import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;

import java.net.URI;
import java.net.URISyntaxException;

public class OlClient {
    private static OpenLineageClient olClient;
    private OlClient() {        
    }
    public static OpenLineageClient getOlClient() {
        if (olClient == null) {
            HttpConfig httpConfig = new HttpConfig();
            try {
                httpConfig.setUrl(new URI("http://localhost:5000"));
                olClient = OpenLineageClient.builder()
                    .transport(new HttpTransport(httpConfig))
                    .build();
            } catch (URISyntaxException e) {
                throw new RuntimeException("Invalid URI for OpenLineage client", e);
            }
        }
        return olClient;
    }
}