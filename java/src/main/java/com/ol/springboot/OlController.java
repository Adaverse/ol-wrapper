package com.ol.springboot;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ol.OlClient.CreateNamespace;
import com.ol.OlClient.OlClient;

import io.openlineage.client.OpenLineageClient;

@RestController
public class OlController {

	@GetMapping("/")
	public String index() {
        OpenLineageClient olClient = OlClient.getOlClient();
        String hi = CreateNamespace.createNamespace("test", "env", "prefix", true);
        System.out.println(hi);
		return "Greetings from Spring Boot!";
	}

}
