package io.kafbat.ui.controller;

import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/environment-config")
public class EnvironmentConfigController {

  @Value("${ENVIRONMENT_IDENTIFIER_LABEL:LOCAL}")
  private String environmentIdentifierLabel;

  @Value("${ENVIRONMENT_IDENTIFIER_COLOR:rgb(0, 71, 255)}")
  private String environmentIdentifierColor;

  @GetMapping
  public Mono<ResponseEntity<Map<String, String>>> getEnvironmentConfig() {
    return Mono.just(
        ResponseEntity.ok(
            Map.of(
                "label", environmentIdentifierLabel,
                "color", environmentIdentifierColor
            )
        )
    );
  }
}

