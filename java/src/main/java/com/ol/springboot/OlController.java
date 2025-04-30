package com.ol.springboot;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ol.OlClient.CreateNamespace;
import com.ol.OlExample.DatasetExample;

@RestController
public class OlController {

	@GetMapping("/")
	public String index() {
        String ns = CreateNamespace.createNamespace("test", "env_java", "prefix", true);
        DatasetExample.emitRunEvent(ns);
		return "Sample dataset event emitted";
	}

}
