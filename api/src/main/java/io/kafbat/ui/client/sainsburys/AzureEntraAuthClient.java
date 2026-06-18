package io.kafbat.ui.client.sainsburys;

import java.net.URI;
import java.util.Map;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;

@FeignClient(name = "azure-entra-auth-client")
public interface AzureEntraAuthClient {

  @PostMapping(value = "", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
  ResponseEntity<Map<String, Object>> getAccessToken(URI absoluteUrl, Map<String, ?> formParams);
}
