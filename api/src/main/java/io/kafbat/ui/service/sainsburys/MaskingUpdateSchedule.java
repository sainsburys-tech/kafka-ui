package io.kafbat.ui.service.sainsburys;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import feign.FeignException;
import io.kafbat.ui.client.sainsburys.AzureEntraAuthClient;
import io.kafbat.ui.client.sainsburys.ConfluentApiClient;
import io.kafbat.ui.config.ClustersProperties;
import io.kafbat.ui.config.sainsburys.ConfluentAuthConfig;
import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.model.sainsburys.SchemaRegistryAuth;
import io.kafbat.ui.model.sainsburys.confluent.ConfluentAvroField;
import io.kafbat.ui.model.sainsburys.confluent.ConfluentAvroSchema;
import io.kafbat.ui.model.sainsburys.confluent.Entity;
import io.kafbat.ui.model.sainsburys.confluent.EntityAttributes;
import io.kafbat.ui.model.sainsburys.confluent.SchemaMetadataResponse;
import io.kafbat.ui.model.sainsburys.confluent.SubjectMetadataResponse;
import io.kafbat.ui.model.sainsburys.confluent.TagDefinitionClassificationResponse;
import io.kafbat.ui.model.sainsburys.dynamo.DynamoMaskingEntity;
import io.kafbat.ui.repository.DynamoMaskingEntityRepository;
import io.kafbat.ui.service.ClustersStorage;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@EnableRetry
public class MaskingUpdateSchedule {

  private final ConfluentApiClient confluentApiClient;
  private final AzureEntraAuthClient azureAuthClient;
  private final ClustersStorage clustersStorage;
  private final DynamoClusterProperties dynamoClusterProperties;
  private final DynamoMaskingEntityRepository dynamoMaskingEntityRepository;

  @Value("${sainsburys.masking.feature.enabled: 'false' }")
  private String isMaskingEnabled;

  @Value("${sainsburys.masking.rule.mask-all-by-default.enabled: 'false' }")
  private String maskAllByDefault;

  @Value("${sainsburys.masking.rule.mask-all-by-default.tag-name: 'NON_PII' }")
  private String defaultNonPiiTag;

  @Value("${sainsburys.masking.rule.chars-replacement: X, x, x, - }")
  private List<String> defaultMaskingCharsReplacement;

  @Value("${sainsburys.masking.rule.topic-replacement: [CONFIDENTIAL] }")
  private String defaultMaskingTopicReplacement;

  @Value("${sainsburys.masking.rule.tags.schema-field: sainsburys.dataClassification }")
  private String schemaDataClassificationTag;

  private static final Pattern CLIENT_ID_PATTERN =
      Pattern.compile("clientId=\"([^\"]+)\"");

  private static final Pattern CLIENT_SECRET_PATTERN =
      Pattern.compile("clientSecret=\"([^\"]+)\"");

  private static final Pattern CLIENT_SCOPE_PATTERN =
      Pattern.compile("scope=\"([^\"]+)\"");


  public MaskingUpdateSchedule(ConfluentApiClient confluentApiClient, AzureEntraAuthClient azureAuthClient,
                               ClustersStorage clustersStorage,
                               DynamoClusterProperties dynamoClusterProperties,
                               DynamoMaskingEntityRepository dynamoMaskingEntityRepository) {
    this.confluentApiClient = confluentApiClient;
    this.azureAuthClient = azureAuthClient;
    this.clustersStorage = clustersStorage;
    this.dynamoClusterProperties = dynamoClusterProperties;
    this.dynamoMaskingEntityRepository = dynamoMaskingEntityRepository;
  }

  @Scheduled(fixedRateString = "${sainsburys.masking.scheduler.update-masking-tags-rate-millis:3000000}",
      initialDelay = 10000)
  protected void executeMasking() {
    try {
      if (Boolean.valueOf(isMaskingEnabled)) {
        log.info("Started MaskClusterStorage: {}", clustersStorage.getKafkaClusters().size());

        AtomicBoolean isMetadataUpdated = new AtomicBoolean(false);

        clustersStorage.getKafkaClusters()
            .forEach(cluster -> {
              try {

                log.info("In cluster MaskClusterStorage: {}", cluster.getName());
                String clusterBaseUrl = cluster.getOriginalProperties().getSchemaRegistry();
                String authUrl = cluster.getProperties() != null
                    ? cluster.getProperties().getProperty("sasl.oauthbearer.token.endpoint.url") : null;

                var clusterAuth = mapperClusterSrAuth(cluster);
                log.info("SchemaRegistry Url MaskClusterStorage: {}",
                    cluster.getOriginalProperties().getSchemaRegistry());

                log.info("MaskClusterStorage maskAllByDefault: {}", Boolean.valueOf(maskAllByDefault));
                if (Boolean.valueOf(maskAllByDefault)) {

                  log.info("Mask all topics by default for cluster: {}", cluster.getName());
                  try {

                    log.info("MaskClusterStorage maskProcessor: {}", Boolean.valueOf(maskAllByDefault));
                    maskProcessor(cluster, clusterAuth, null, isMetadataUpdated);
                  } catch (Exception e) {
                    log.error("MaskClusterStorage Failed maskProcessor cluster masking {} message: {}",
                        cluster.getName(), e.getMessage());
                  }

                } else {

                  log.info("MaskClusterStorage get tagDefinitionList");
                  List<TagDefinitionClassificationResponse> tagDefinitionList = tagDefinitionResponse(clusterBaseUrl,
                      authUrl,
                      clusterAuth);

                  if (tagDefinitionList != null && !tagDefinitionList.isEmpty()) {
                    log.info("MaskClusterStorage processing tagDefinitionList");
                    tagDefinitionList.stream().map(TagDefinitionClassificationResponse::getName)
                        .forEach(tag -> {
                          log.info("MaskClusterStorage Tag found for cluster: {}, tag: {}", cluster.getName(), tag);
                          try {
                            log.info("MaskClusterStorage maskProcessor: {}", Boolean.valueOf(maskAllByDefault));
                            maskProcessor(cluster, clusterAuth, tag, isMetadataUpdated);
                          } catch (Exception e) {
                            log.error("MaskClusterStorage Failed maskProcessor cluster masking {} message: {}",
                                cluster.getName(),
                                e.getMessage());
                          }
                        });
                  }
                }

              } catch (Exception e) {
                log.info("MaskClusterStorage Failed processing cluster masking {} message: {}", cluster.getName(),
                    e.getMessage());
              }
            });

        if (isMetadataUpdated.get()) {
          log.info("MaskClusterStorage DynamoDB Masking Config Refresh");
          dynamoClusterProperties.loadMaskingConfiguration();
        }
      } else {
        log.info("Masking feature not enabled as yet, configure in Application properties.");
      }
    } catch (Exception e) {
      log.error("MaskClusterStorage Job Failed with message: {}", e.getMessage());
      throw new RuntimeException(e);
    }
    log.info("MaskClusterStorage Masking Job Completed");
  }

  private void maskProcessor(KafkaCluster cluster,
                             SchemaRegistryAuth authentication,
                             String tag,
                             AtomicBoolean isMetadataUpdated) {
    String baseUrl = cluster.getOriginalProperties().getSchemaRegistry();
    String authUrl = cluster.getProperties() != null
        ? cluster.getProperties().getProperty("sasl.oauthbearer.token.endpoint.url") : null;

    log.info("MaskClusterStorage Fetch Topics for cluster: {}", cluster.getName());
    SchemaMetadataResponse confluentResponse = metadataTopicResponses(baseUrl, authUrl, authentication, tag);
    List<String> confluentSubjectsList = new ArrayList<>();
    List<EntityAttributes> confluentTopicList = new ArrayList<>();

    if (confluentResponse == null) {
      log.info("MaskClusterStorage Failed to fetch confluent topics for cluster: {}, baseUrl: {} and tag: {}",
          cluster.getName(), baseUrl, tag);
      confluentSubjectsList = metadataTopicList(baseUrl, authUrl, authentication);
      log.info("MaskClusterStorage fetch confluent subjects for cluster: {}, baseUrl: {} and topics null: {}",
          cluster.getName(), baseUrl, confluentSubjectsList == null);
      if (confluentSubjectsList == null) {
        throw new ValidationException("MaskClusterStorage Topics not found for cluster: " + cluster.getName());
      }

    } else {
      if (tag == null) {
        log.info("MaskClusterStorage Processing No Tag Topics for cluster: {}", cluster.getName());
        confluentTopicList = confluentResponse.getEntities().stream()
            .filter(Predicate.not(e -> e.getClassificationNames().contains(defaultNonPiiTag)))
            .map(Entity::getAttributes).filter(Objects::nonNull)
            .toList();
      } else {
        log.info("MaskClusterStorage Processing Tag: {} Topics for cluster: {}", tag, cluster.getName());
        confluentTopicList = confluentResponse.getEntities().stream()
            .filter(e -> e.getClassificationNames().contains(tag))
            .map(Entity::getAttributes).filter(Objects::nonNull)
            .toList();
      }
    }

    if (!confluentTopicList.isEmpty()) {
      processConfluentTopicMetadataList(cluster, authentication, isMetadataUpdated, confluentTopicList, baseUrl,
          authUrl);
    }

    if (!confluentSubjectsList.isEmpty()) {
      processConfluentSubjectsList(cluster, authentication, isMetadataUpdated, confluentSubjectsList, baseUrl,
          authUrl);
    }
  }

  private void processConfluentTopicMetadataList(KafkaCluster cluster,
                                                 SchemaRegistryAuth authentication,
                                                 AtomicBoolean isMetadataUpdated,
                         List<EntityAttributes> confluentTopicList, String baseUrl, String authUrl) {
    confluentTopicList.forEach(topic -> {
      try {

        log.info("MaskClusterStorage Processing Topic: {} for cluster: {}", topic.getName(), cluster.getName());
        if (topic.getQualifiedName() != null) {
          if (cluster.getOriginalProperties().getMasking() == null
              || cluster.getOriginalProperties().getMasking().isEmpty()) {

            log.info("MaskClusterStorage Fetch Topic: {} Metadata", topic.getName());
            SubjectMetadataResponse confluentTopicFieldsResponse = retrieveSubjectMetadataResponses(baseUrl, authUrl,
                authentication,
                topic.getName());

            if (confluentTopicFieldsResponse != null
                && confluentTopicFieldsResponse.getSchema().contains(schemaDataClassificationTag)) {
              log.info("MaskClusterStorage Field Mask Topic: {}", topic.getName());
              updateFieldLevelMasking(cluster, topic.getName(),
                  new ClustersProperties.Masking(),
                  isMetadataUpdated,
                  confluentTopicFieldsResponse);
            } else {
              log.info("MaskClusterStorage Topic Mask: {}", topic.getName());
              updateTopicLevelMasking(cluster, topic.getName(),
                  new ClustersProperties.Masking(),
                  isMetadataUpdated,
                  confluentTopicFieldsResponse);
            }
          } else {
            log.info("MaskClusterStorage Fetch2 Topic: {} Metadata", topic.getName());
            SubjectMetadataResponse confluentTopicFieldsResponse = retrieveSubjectMetadataResponses(baseUrl, authUrl,
                authentication,
                topic.getName());

            if (confluentTopicFieldsResponse != null
                && confluentTopicFieldsResponse.getSchema().contains(schemaDataClassificationTag)) {
              log.info("MaskClusterStorage Field2 Mask Topic: {}", topic.getName());
              List<ClustersProperties.@Valid Masking> fieldMaskList =
                  cluster.getOriginalProperties().getMasking().stream()
                      .filter(mask -> mask.getType().equals(
                          ClustersProperties.Masking.Type.MASK))
                      .filter(mask -> mask.getTopicValuesPattern().equalsIgnoreCase(topic.getName()))
                      .toList();

              if (!fieldMaskList.isEmpty()) {
                fieldMaskList.forEach(mask -> {
                  updateFieldLevelMasking(cluster, topic.getName(), mask, isMetadataUpdated,
                      confluentTopicFieldsResponse);
                });
              } else {
                updateFieldLevelMasking(cluster, topic.getName(),
                    new ClustersProperties.Masking(),
                    isMetadataUpdated, confluentTopicFieldsResponse);
              }
            } else {
              log.info("MaskClusterStorage Topic2 Mask: {}", topic.getName());
              List<ClustersProperties.@Valid Masking> topicMaskList =
                  cluster.getOriginalProperties().getMasking().stream()
                      .filter(mask -> mask.getType().equals(
                          ClustersProperties.Masking.Type.REPLACE))
                      .filter(mask -> mask.getTopicValuesPattern().equalsIgnoreCase(topic.getName()))
                      .toList();

              if (!topicMaskList.isEmpty()) {
                topicMaskList.forEach(mask -> {
                  updateTopicLevelMasking(cluster, topic.getName(), mask,
                      isMetadataUpdated,
                      confluentTopicFieldsResponse);
                });
              } else {
                updateTopicLevelMasking(cluster, topic.getName(),
                    new ClustersProperties.Masking(),
                    isMetadataUpdated,
                    confluentTopicFieldsResponse);
              }
            }
          }
        }


      } catch (Exception e) {
        log.info("MaskClusterStorage Failed Processing Topic: {}, Message: {}", topic.getName(), e.getMessage());
      }
    });
  }

  private void processConfluentSubjectsList(KafkaCluster cluster,
                                                 SchemaRegistryAuth authentication,
                                                 AtomicBoolean isMetadataUpdated,
                                                 List<String> confluentTopicList, String baseUrl, String authUrl) {
    confluentTopicList.forEach(topic -> {
      try {

        log.info("MaskClusterStorage Processing Topic: {} for cluster: {}", topic, cluster.getName());
        if (cluster.getOriginalProperties().getMasking() == null
            || cluster.getOriginalProperties().getMasking().isEmpty()) {

          log.info("MaskClusterStorage Fetch Topic: {} Metadata", topic);
          SubjectMetadataResponse confluentTopicFieldsResponse = retrieveSubjectMetadataResponses(baseUrl, authUrl,
              authentication,
              topic);

          if (confluentTopicFieldsResponse != null
              && confluentTopicFieldsResponse.getSchema().contains(schemaDataClassificationTag)) {
            log.info("MaskClusterStorage Field Mask Topic: {}", topic);
            updateFieldLevelMasking(cluster, topic,
                new ClustersProperties.Masking(),
                isMetadataUpdated,
                confluentTopicFieldsResponse);
          } else {
            log.info("MaskClusterStorage Topic Mask: {}", topic);
            updateTopicLevelMasking(cluster, topic,
                new ClustersProperties.Masking(),
                isMetadataUpdated,
                confluentTopicFieldsResponse);
          }
        } else {
          log.info("MaskClusterStorage Fetch2 Topic: {} Metadata", topic);
          SubjectMetadataResponse confluentTopicFieldsResponse = retrieveSubjectMetadataResponses(baseUrl, authUrl,
              authentication,
              topic);

          if (confluentTopicFieldsResponse != null
              && confluentTopicFieldsResponse.getSchema().contains(schemaDataClassificationTag)) {
            log.info("MaskClusterStorage Field2 Mask Topic: {}", topic);
            List<ClustersProperties.@Valid Masking> fieldMaskList =
                cluster.getOriginalProperties().getMasking().stream()
                    .filter(mask -> mask.getType().equals(
                        ClustersProperties.Masking.Type.MASK))
                    .filter(mask -> mask.getTopicValuesPattern().equalsIgnoreCase(topic))
                    .toList();

            if (!fieldMaskList.isEmpty()) {
              fieldMaskList.forEach(mask -> {
                updateFieldLevelMasking(cluster, topic, mask, isMetadataUpdated,
                    confluentTopicFieldsResponse);
              });
            } else {
              updateFieldLevelMasking(cluster, topic,
                  new ClustersProperties.Masking(),
                  isMetadataUpdated, confluentTopicFieldsResponse);
            }
          } else {
            log.info("MaskClusterStorage Topic2 Mask: {}", topic);
            List<ClustersProperties.@Valid Masking> topicMaskList =
                cluster.getOriginalProperties().getMasking().stream()
                    .filter(mask -> mask.getType().equals(
                        ClustersProperties.Masking.Type.REPLACE))
                    .filter(mask -> mask.getTopicValuesPattern().equalsIgnoreCase(topic))
                    .toList();

            if (!topicMaskList.isEmpty()) {
              topicMaskList.forEach(mask -> {
                updateTopicLevelMasking(cluster, topic, mask,
                    isMetadataUpdated,
                    confluentTopicFieldsResponse);
              });
            } else {
              updateTopicLevelMasking(cluster, topic,
                  new ClustersProperties.Masking(),
                  isMetadataUpdated,
                  confluentTopicFieldsResponse);
            }
          }
        }
      } catch (Exception e) {
        log.info("MaskClusterStorage Failed Processing Topic: {}, Message: {}", topic, e.getMessage());
      }
    });
  }

  private void updateTopicLevelMasking(KafkaCluster cluster, String topic,
                                       ClustersProperties.@Valid Masking mask,
                                       AtomicBoolean isMetadataUpdated,
                                       SubjectMetadataResponse confluentTopicFieldsResponse) {
    log.info("MaskClusterStorage Topic Level Masking for topic name: {}", topic);
    if (confluentTopicFieldsResponse == null) {
      log.error("MaskClusterStorage Topic schema not found");
      mask.setType(ClustersProperties.Masking.Type.REPLACE);
      mask.setReplacement(defaultMaskingTopicReplacement);
      mask.setTopicValuesPattern(topic);
      isMetadataUpdated.set(true);
      saveMaskingEntity(mapperMaskingDtoToEntity(cluster.getName(), mask));
      throw new ValidationException("MaskClusterStorage Topic schema not found for topic: " + topic);
    }
    ConfluentAvroSchema confluentAvroSchema = avroSchemaMapper(confluentTopicFieldsResponse.getSchema());

    if (confluentAvroSchema != null && !confluentAvroSchema.getFields().isEmpty()) {

      log.info("MaskClusterStorage Processing fields from confluent subject: {}",
          confluentTopicFieldsResponse.getSubject());
      mask.setType(ClustersProperties.Masking.Type.REPLACE);
      mask.setReplacement(defaultMaskingTopicReplacement);
      mask.setTopicValuesPattern(topic);

      List<String> confluentTopicFieldsList = confluentAvroSchema.getFields()
          .stream()
          .map(ConfluentAvroField::getName)
          .map(String::toLowerCase)
          .toList();

      List<String> fieldsToMask = mask.getFields().isEmpty() ? confluentTopicFieldsList : fieldsToMask(mask.getFields(),
          confluentTopicFieldsList);
      log.info("MaskClusterStorage confluent fields to mask null: {}, and list: {}",
          fieldsToMask == null, fieldsToMask);

      if (!fieldsToMask.isEmpty()) {
        fieldsToMask.stream().forEach(field -> {
          mask.getFields().add(field);
          isMetadataUpdated.set(true);
        });
      }
      List<String> fieldsToRemove = mask.getFields().isEmpty() ? mask.getFields() :
          fieldsToRemoveFromMask(mask.getFields(), confluentTopicFieldsList);
      if (!fieldsToRemove.isEmpty()) {
        fieldsToRemove.stream().forEach(field -> {
          mask.getFields().remove(field);
          isMetadataUpdated.set(true);
        });
      }
      if (cluster.getOriginalProperties().getMasking() != null
          && !cluster.getOriginalProperties().getMasking().contains(mask)) {
        log.info("MaskClusterStorage completed and saving cluster: {}",
            cluster.getName());
        saveMaskingEntity(mapperMaskingDtoToEntity(cluster.getName(), mask));
      }

    }
  }

  private void updateFieldLevelMasking(KafkaCluster cluster, String topic,
                                       ClustersProperties.@Valid @MonotonicNonNull Masking mask,
                                       AtomicBoolean isMetadataUpdated,
                                       SubjectMetadataResponse confluentTopicFieldsResponse) {
    List<String> currentFields = mask.getFields();
    log.info("MaskClusterStorage Field Level Masking for topic name: {}", topic);
    ConfluentAvroSchema confluentAvroSchema = avroSchemaMapper(confluentTopicFieldsResponse.getSchema());

    List<String> confluentEntityList = confluentAvroSchema.getFields().stream()
        .filter(field -> !field.getCustomTags().isEmpty()
            && field.getCustomTags().get(schemaDataClassificationTag) != null)
        .map(ConfluentAvroField::getName)
        .toList();

    if (!confluentEntityList.isEmpty()) {
      log.info("MaskClusterStorage Processing fields from confluent: {}", confluentEntityList);
      mask.setType(ClustersProperties.Masking.Type.MASK);
      mask.setMaskingCharsReplacement(defaultMaskingCharsReplacement);
      mask.setTopicValuesPattern(topic);

      List<String> fieldsToMask = currentFields.isEmpty() ? confluentEntityList : fieldsToMask(currentFields,
          confluentEntityList);

      if (!fieldsToMask.isEmpty()) {
        fieldsToMask.stream().forEach(field -> {
          mask.getFields().add(field);
          isMetadataUpdated.set(true);
        });
      }

      List<String> fieldsToRemove = currentFields.isEmpty() ? currentFields : fieldsToRemoveFromMask(currentFields,
          confluentEntityList);

      if (!fieldsToRemove.isEmpty()) {
        fieldsToRemove.stream().forEach(field -> {
          mask.getFields().remove(field);
          isMetadataUpdated.set(true);
        });
      }
      if (!cluster.getOriginalProperties().getMasking().contains(mask)) {
        cluster.getOriginalProperties().getMasking().add(mask);
        saveMaskingEntity(mapperMaskingDtoToEntity(cluster.getName(), mask));
      }
    }
  }

  private List<String> fieldsToMask(List<String> dynamicConfigFields, List<String> schemaRegistryFields) {
    return  schemaRegistryFields.stream()
        .filter(item -> !dynamicConfigFields.contains(item))
        .toList();
  }

  private List<String> fieldsToRemoveFromMask(List<String> dynamicConfigFields, List<String> schemaRegistryFields) {
    return dynamicConfigFields.stream()
        .filter(item -> !schemaRegistryFields.contains(item))
        .toList();
  }

  @Retryable(
      retryFor = { FeignException.class },
      backoff = @Backoff(delay = 2000, multiplier = 2)
  )
  private List<TagDefinitionClassificationResponse> tagDefinitionResponse(String baseUrl, String authUrl,
                @MonotonicNonNull SchemaRegistryAuth authentication) {
    try {
      log.info("MaskClusterStorage tagDefinitionResponse");
      if (baseUrl != null && authentication != null) {
        String authorization = null;
        if (authentication.scope() == null) {
          authorization = ConfluentAuthConfig.generateBasicAuthentication(authentication.username(),
              authentication.password());
        } else {
          authorization = generateBearerToken(authUrl, authentication.username(), authentication.password(),
              authentication.scope());
        }

        ResponseEntity<List<TagDefinitionClassificationResponse>> tagDefinitions =
            confluentApiClient.retrieveTagDefinitions(URI.create(baseUrl), authorization,
                URI.create(baseUrl).getHost());

        if (tagDefinitions != null && tagDefinitions.getStatusCode().is2xxSuccessful()) {
          return tagDefinitions.getBody();
        }
      }
    } catch (FeignException e) {
      log.error("MaskClusterStorage Feign API call error with message: {}", e.getMessage());
    }
    return null;
  }

  @Retryable(
      retryFor = { FeignException.class },
      backoff = @Backoff(delay = 2000, multiplier = 2)
  )
  private SchemaMetadataResponse metadataTopicResponses(String baseUrl, String authUrl,
                            @MonotonicNonNull SchemaRegistryAuth authentication,
                            String tag) {
    try {
      log.info("MaskClusterStorage metadataTopicResponses");
      String authorization = null;
      if (authentication != null && authentication.scope() == null) {
        authorization = ConfluentAuthConfig.generateBasicAuthentication(authentication.username(),
            authentication.password());
      } else {
        authorization = generateBearerToken(authUrl, authentication.username(), authentication.password(),
            authentication.scope());
      }
      ResponseEntity<SchemaMetadataResponse> metadata = null;

      if (tag == null) {
        metadata = confluentApiClient.retrieveTopicMetadata(URI.create(baseUrl),
            authorization, URI.create(baseUrl).getHost());
      } else {
        metadata = confluentApiClient.retrieveTopicMetadata(URI.create(baseUrl),
            authorization, tag, URI.create(baseUrl).getHost());
      }
      if (metadata != null && metadata.getStatusCode().is2xxSuccessful()) {
        return metadata.getBody();
      }
    } catch (FeignException e) {
      log.error("MaskClusterStorage Feign API call error with message: {}", e.getMessage());

    }
    return null;
  }

  @Retryable(
      retryFor = { FeignException.class },
      backoff = @Backoff(delay = 2000, multiplier = 2)
  )
  private List<String> metadataTopicList(String baseUrl, String authUrl,
                                                        @MonotonicNonNull SchemaRegistryAuth authentication) {
    try {
      log.info("MaskClusterStorage metadataTopicList");
      String authorization = null;
      if (authentication != null && authentication.scope() == null) {
        authorization = ConfluentAuthConfig.generateBasicAuthentication(authentication.username(),
            authentication.password());
      } else {
        authorization = generateBearerToken(authUrl, authentication.username(), authentication.password(),
            authentication.scope());
      }
      ResponseEntity<List<String>> metadata =  confluentApiClient.retrieveTopicList(URI.create(baseUrl),
            null, URI.create(baseUrl).getHost());

      log.info("MaskClusterStorage confluent topic response: {}, body: {}",
          metadata, metadata != null ? metadata.getBody() : null);
      if (metadata != null && metadata.getStatusCode().is2xxSuccessful()) {
        return metadata.getBody();
      }
    } catch (FeignException e) {
      log.error("MaskClusterStorage Feign API Status: {}, Body: {}", e.status(), e.contentUTF8());
      log.error("MaskClusterStorage Feign API call error with message: {}", e.getMessage());

    }
    return null;
  }

  @Retryable(
      retryFor = { FeignException.class },
      backoff = @Backoff(delay = 2000, multiplier = 2)
  )
  private SubjectMetadataResponse retrieveSubjectMetadataResponses(String baseUrl, String authUrl,
                                @MonotonicNonNull SchemaRegistryAuth authentication,
                                String topic) {
    try {
      log.info("MaskClusterStorage retrieveSubjectMetadataResponses");
      String authorization = null;
      if (authentication != null && authentication.scope() == null) {
        authorization = ConfluentAuthConfig.generateBasicAuthentication(authentication.username(),
            authentication.password());
      } else {
        authorization = generateBearerToken(authUrl, authentication.username(), authentication.password(),
            authentication.scope());
      }
      ResponseEntity<SubjectMetadataResponse> metadata =
          confluentApiClient.retrieveSubjectMetadata(URI.create(baseUrl), null, topic,
              URI.create(baseUrl).getHost());
      if (metadata != null && metadata.getStatusCode().is2xxSuccessful()) {
        return metadata.getBody();
      }
    } catch (FeignException e) {
      log.error("MaskClusterStorage Feign API call error with message: {}", e.getMessage());
    }
    return null;
  }

  private ConfluentAvroSchema avroSchemaMapper(String schema) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(schema, ConfluentAvroSchema.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Retryable(
      retryFor = {
          ProvisionedThroughputExceededException.class,
          SdkClientException.class
      },
      backoff = @Backoff(delay = 500, multiplier = 2)
  )
  private void saveMaskingEntity(DynamoMaskingEntity mask) {
    try {
      log.info("MaskClusterStorage saveMaskingEntity");
      dynamoMaskingEntityRepository.save(mask);
      log.info("MaskClusterStorage Dynamo Mask saved successfully. {}", mask.getName());
    } catch (Exception e) {
      log.error("MaskClusterStorage Dynamo Masking config: {} failed with message: {}", mask.getName(),
          e.getMessage());
    }
  }

  private DynamoMaskingEntity mapperMaskingDtoToEntity(String cluster,
                                                       ClustersProperties.@Valid @MonotonicNonNull Masking source) {
    DynamoMaskingEntity target = new DynamoMaskingEntity();
    if (source.getTopicValuesPattern() != null) {
      target.setName(String.format("%s_%s", cluster, source.getTopicValuesPattern()));
    } else {
      target.setName(String.format("%s_%s", cluster, source.getTopicKeysPattern()));
    }
    target.setType(source.getType().name());
    target.setReplacement(source.getReplacement());
    target.setMaskingCharsReplacement(source.getMaskingCharsReplacement());
    target.setTopicKeysPattern(source.getTopicKeysPattern());
    target.setTopicValuesPattern(source.getTopicValuesPattern());
    target.setFields(source.getFields());
    return target;

  }

  private SchemaRegistryAuth mapperClusterSrAuth(KafkaCluster source) {
    var clusterAuth = source.getOriginalProperties().getSchemaRegistryAuth();
    if (clusterAuth != null) {
      return new SchemaRegistryAuth(clusterAuth.getUsername(), clusterAuth.getPassword(), null);
    } else {
      return extractSchemaRegistryAuth(source.getProperties().getProperty("sasl.jaas.config"));
    }
  }

  public static SchemaRegistryAuth extractSchemaRegistryAuth(String jaasConfig) {
    String clientId = extract(CLIENT_ID_PATTERN, jaasConfig);
    String clientSecret = extract(CLIENT_SECRET_PATTERN, jaasConfig);
    String scope = extract(CLIENT_SCOPE_PATTERN, jaasConfig);

    return new SchemaRegistryAuth(clientId, clientSecret, scope);
  }

  private static String extract(Pattern pattern, String value) {
    Matcher matcher = pattern.matcher(value);
    if (matcher.find()) {
      return matcher.group(1);
    }

    throw new IllegalArgumentException(
        "Could not find " + pattern.pattern() + " in JAAS config");
  }

  public String generateBearerToken(String baseUrl, String clientId, String clientSecret, String scope) {
    log.info("MaskClusterStorage Generate Bearer Token authUrl: {}, scope: {}", baseUrl, scope);
    if (!baseUrl.endsWith("/token")) {
      baseUrl = baseUrl.endsWith("/") ? baseUrl + "token" : baseUrl + "/token";
    }
    URI baseUri = URI.create(baseUrl);
    Map<String, String> formPayload = new HashMap<>();
    formPayload.put("grant_type", "client_credentials");
    formPayload.put("client_id", clientId);
    formPayload.put("client_secret", clientSecret);
    formPayload.put("scope", scope != null ? scope : "https://graph.microsoft.com/.default");

    try {
      ResponseEntity<Map<String, Object>> response = azureAuthClient.getAccessToken(baseUri, formPayload);

      if (response != null && response.getStatusCode().is2xxSuccessful()
          && response.getBody().containsKey("access_token")) {
        log.info("MaskClusterStorage Successfully Generated Bearer Token authUrl: {}, scope: {}", baseUrl, scope);

        return (String) response.getBody().get("access_token");
      }
      log.info("MaskClusterStorage Authentication Failed Generate Bearer Token authUrl: {}, scope: {}", baseUrl, scope);

      throw new IllegalStateException("MaskClusterStorage Authentication failed: "
          + "'access_token' was missing from response.");

    } catch (Exception e) {
      log.info("MaskClusterStorage Failed To Generate Bearer Token authUrl: {}, scope: {}", baseUrl, scope);

      throw new RuntimeException("MaskClusterStorage Failed to pull token against absolute URL target: " + baseUrl, e);
    }

  }
}
