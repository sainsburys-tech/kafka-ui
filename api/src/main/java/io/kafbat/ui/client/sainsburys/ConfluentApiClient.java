package io.kafbat.ui.client.sainsburys;

import io.kafbat.ui.model.sainsburys.confluent.SchemaMetadataResponse;
import io.kafbat.ui.model.sainsburys.confluent.SubjectMetadataResponse;
import io.kafbat.ui.model.sainsburys.confluent.TagDefinitionClassificationResponse;
import java.net.URI;
import java.util.List;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "confluent-api-client")
public interface ConfluentApiClient {

  @GetMapping(path = "${sainsburys.external.services.confluent-api.operations.retrieve.tagdefs}",
      headers = {
        "Content-Type=application/vnd.schemaregistry.v1+json",
        "Accept=*/*"
      })
  ResponseEntity<List<TagDefinitionClassificationResponse>> retrieveTagDefinitions(URI baseUrl,
                                              @RequestHeader("Authorization") String authorization,
                                              @RequestHeader("Host") String host);

  @GetMapping(path = "${sainsburys.external.services.confluent-api.operations.retrieve.topic}",
      headers = {
        "Content-Type=application/vnd.schemaregistry.v1+json",
        "Accept=*/*"
      })
  ResponseEntity<SchemaMetadataResponse> retrieveTopicMetadata(URI baseUrl,
                                            @RequestHeader("Authorization") String authorization,
                                            @RequestParam("tag") String tag,
                                            @RequestHeader("Host") String host);

  @GetMapping(path = "${sainsburys.external.services.confluent-api.operations.retrieve.topic}",
      headers = {
        "Content-Type=application/vnd.schemaregistry.v1+json",
        "Accept=*/*"
      })
  ResponseEntity<SchemaMetadataResponse> retrieveTopicMetadata(URI baseUrl,
                                                               @RequestHeader("Authorization") String authorization,
                                                               @RequestHeader("Host") String host);

  @GetMapping(path = "${sainsburys.external.services.confluent-api.operations.retrieve.subjects}",
      headers = {
        "Content-Type=application/vnd.schemaregistry.v1+json",
        "Accept=*/*"
      })
  ResponseEntity<List<String>> retrieveTopicList(URI baseUrl,
                                                 @RequestHeader("Authorization") String authorization,
                                                 @RequestHeader("Host") String host);

  @GetMapping(path = "${sainsburys.external.services.confluent-api.operations.retrieve.topic.fields}",
      headers = {
        "Content-Type=application/vnd.schemaregistry.v1+json",
        "Accept=*/*"
      })
  ResponseEntity<SchemaMetadataResponse> retrieveTopicFieldsMetadata(URI baseUrl,
                                        @RequestHeader("Authorization") String authorization,
                                        @RequestParam("tag") String tag,
                                        @RequestHeader("Host") String host);

  @GetMapping(path = "${sainsburys.external.services.confluent-api.operations.retrieve.schema}",
      headers = {
        "Content-Type=application/vnd.schemaregistry.v1+json",
        "Accept=*/*"
      })
  ResponseEntity<SchemaMetadataResponse> retrieveSchemaMetadata(URI baseUrl,
                                                                @RequestHeader("Authorization") String authorization,
                                                                @RequestHeader("Host") String host);

  @GetMapping(path = "${sainsburys.external.services.confluent-api.operations.retrieve.subject}",
      headers = {
        "Content-Type=application/vnd.schemaregistry.v1+json",
        "Accept=*/*"
      })
  ResponseEntity<SubjectMetadataResponse> retrieveSubjectMetadata(URI baseUrl,
                                                                  @RequestHeader("Authorization") String authorization,
                                                                  @PathVariable("topic") String subject,
                                                                  @RequestHeader("Host") String host);
}
