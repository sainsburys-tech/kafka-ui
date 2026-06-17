package io.kafbat.ui.service.sainsburys;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import feign.FeignException;
import io.kafbat.ui.client.sainsburys.ConfluentApiClient;
import io.kafbat.ui.config.ClustersProperties;
import io.kafbat.ui.config.sainsburys.ConfluentAuthConfig;
import io.kafbat.ui.model.ApplicationConfigPropertiesKafkaClustersInnerMaskingInnerDTO;
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
import java.util.List;
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


  public MaskingUpdateSchedule(ConfluentApiClient confluentApiClient,
                               ClustersStorage clustersStorage,
                               DynamoClusterProperties dynamoClusterProperties,
                               DynamoMaskingEntityRepository dynamoMaskingEntityRepository) {
    this.confluentApiClient = confluentApiClient;
    this.clustersStorage = clustersStorage;
    this.dynamoClusterProperties = dynamoClusterProperties;
    this.dynamoMaskingEntityRepository = dynamoMaskingEntityRepository;
  }

  @Scheduled(fixedRateString = "${sainsburys.masking.scheduler.update-masking-tags-rate-millis:3000000}",
      initialDelay = 10000)
  protected void executeMasking() {
    try {
      if (Boolean.valueOf(isMaskingEnabled)) {
        log.info("Update masking tags dynamic config start");
        log.info("Processing MaskClusterStorage: {}", clustersStorage.getKafkaClusters().isEmpty());

        AtomicBoolean isMetadataUpdated = new AtomicBoolean(false);

        clustersStorage.getKafkaClusters()
            .forEach(cluster -> {
              String clusterBaseUrl = cluster.getOriginalProperties().getSchemaRegistry();
              var clusterAuth = mapperClusterSrAuth(cluster);

              if (Boolean.valueOf(maskAllByDefault)) {

                log.info("Mask all topics by default for cluster: {}", cluster.getName());
                try {
                  maskProcessor(cluster, clusterAuth, null, isMetadataUpdated);
                } catch (Exception e) {
                  log.error("Failed processing cluster masking {} message: {}", cluster.getName(), e.getMessage());
                }

              } else {
                List<TagDefinitionClassificationResponse> tagDefinitionList = tagDefinitionResponse(clusterBaseUrl,
                    clusterAuth);

                if (tagDefinitionList != null && !tagDefinitionList.isEmpty()) {
                  tagDefinitionList.stream().map(TagDefinitionClassificationResponse::getName)
                      .forEach(tag -> {
                        log.info("Tag found for cluster: {}, tag: {}", cluster.getName(), tag);
                        try {
                          maskProcessor(cluster, clusterAuth, tag, isMetadataUpdated);
                        } catch (Exception e) {
                          log.error("Failed processing cluster masking {} message: {}", cluster.getName(),
                              e.getMessage());
                        }
                      });
                }
              }
            });

        if (isMetadataUpdated.get()) {
          log.info("DynamoDB Masking Config Refresh");
          dynamoClusterProperties.loadMaskingConfiguration();
        }
      } else {
        log.info("Masking feature not enabled as yet, configure in Application properties.");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void maskProcessor(KafkaCluster cluster,
                             SchemaRegistryAuth authentication,
                             String tag,
                             AtomicBoolean isMetadataUpdated) {
    String baseUrl = cluster.getOriginalProperties().getSchemaRegistry();
    SchemaMetadataResponse confluentResponse = metadataTopicResponses(baseUrl, authentication, tag);

    if (confluentResponse == null) {
      log.error("Tag metadata API did not return correctly for baseUrl: {} and tag: {}", baseUrl, tag);
      return;
    }

    List<EntityAttributes> confluentTopicList = new ArrayList<>();

    if (tag == null) {
      confluentTopicList = confluentResponse.getEntities().stream()
          .filter(Predicate.not(e -> e.getClassificationNames().contains(defaultNonPiiTag)))
          .map(Entity::getAttributes).filter(Objects::nonNull)
          .toList();
    } else {
      confluentTopicList = confluentResponse.getEntities().stream()
          .filter(e -> e.getClassificationNames().contains(tag))
          .map(Entity::getAttributes).filter(Objects::nonNull)
          .toList();
    }

    confluentTopicList.forEach(topic -> {
      if (topic.getQualifiedName() != null) {
        if (cluster.getOriginalProperties().getMasking().isEmpty()) {

          SubjectMetadataResponse confluentTopicFieldsResponse = retrieveSubjectMetadataResponses(baseUrl,
              authentication,
              topic.getName());

          if (confluentTopicFieldsResponse != null
              && confluentTopicFieldsResponse.getSchema().contains(schemaDataClassificationTag)) {
            updateFieldLevelMasking(cluster, topic.getName(),
                new ClustersProperties.Masking(),
                isMetadataUpdated,
                confluentTopicFieldsResponse);
          } else {
            updateTopicLevelMasking(cluster, topic.getName(),
                new ClustersProperties.Masking(),
                isMetadataUpdated,
                confluentTopicFieldsResponse);
          }
        } else {
          SubjectMetadataResponse confluentTopicFieldsResponse = retrieveSubjectMetadataResponses(baseUrl,
              authentication,
              topic.getName());

          if (confluentTopicFieldsResponse != null
              && confluentTopicFieldsResponse.getSchema().contains(schemaDataClassificationTag)) {
            List<ClustersProperties.@Valid Masking> fieldMaskList =
                cluster.getOriginalProperties().getMasking().stream()
                    .filter(mask -> mask.getType().equals(
                        ApplicationConfigPropertiesKafkaClustersInnerMaskingInnerDTO.TypeEnum.MASK))
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
            List<ClustersProperties.@Valid Masking> topicMaskList =
                cluster.getOriginalProperties().getMasking().stream()
                    .filter(mask -> mask.getType().equals(
                        ApplicationConfigPropertiesKafkaClustersInnerMaskingInnerDTO.TypeEnum.REPLACE))
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

    });
  }

  private void updateTopicLevelMasking(KafkaCluster cluster, String topic,
                                       ClustersProperties.@Valid Masking mask,
                                       AtomicBoolean isMetadataUpdated,
                                       SubjectMetadataResponse confluentTopicFieldsResponse) {
    log.info("Topic Level Masking for topic name: {}", topic);
    if (confluentTopicFieldsResponse == null) {
      log.error("Topic schema not found");
      return;
    }
    ConfluentAvroSchema confluentAvroSchema = avroSchemaMapper(confluentTopicFieldsResponse.getSchema());

    if (confluentAvroSchema != null && !confluentAvroSchema.getFields().isEmpty()) {

      log.info("Processing fields from confluent subject: {}", confluentTopicFieldsResponse.getSubject());
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
      if (!cluster.getOriginalProperties().getMasking().contains(mask)) {
        cluster.getOriginalProperties().getMasking().add(mask);
        saveMaskingEntity(mapperMaskingDtoToEntity(cluster.getName(), mask));
      }

    }
  }

  private void updateFieldLevelMasking(KafkaCluster cluster, String topic,
                                       ClustersProperties.@Valid @MonotonicNonNull Masking mask,
                                       AtomicBoolean isMetadataUpdated,
                                       SubjectMetadataResponse confluentTopicFieldsResponse) {
    List<String> currentFields = mask.getFields();
    log.info("Field Level Masking for topic name: {}", topic);
    ConfluentAvroSchema confluentAvroSchema = avroSchemaMapper(confluentTopicFieldsResponse.getSchema());

    List<String> confluentEntityList = confluentAvroSchema.getFields().stream()
        .filter(field -> !field.getCustomTags().isEmpty()
            && field.getCustomTags().get(schemaDataClassificationTag) != null)
        .map(ConfluentAvroField::getName)
        .toList();

    if (!confluentEntityList.isEmpty()) {
      log.info("Processing fields from confluent: {}", confluentEntityList);
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
  private List<TagDefinitionClassificationResponse> tagDefinitionResponse(String baseUrl,
                @MonotonicNonNull SchemaRegistryAuth authentication) {
    try {
      if (baseUrl != null && authentication != null) {
        String authorization = ConfluentAuthConfig.generateBasicAuthentication(authentication.username(),
            authentication.password());

        ResponseEntity<List<TagDefinitionClassificationResponse>> tagDefinitions =
            confluentApiClient.retrieveTagDefinitions(URI.create(baseUrl), authorization);

        if (tagDefinitions != null && tagDefinitions.getStatusCode().is2xxSuccessful()) {
          return tagDefinitions.getBody();
        }
      }
    } catch (FeignException e) {
      log.error("Feign API call error with message: {}", e.getMessage());
    }
    return null;
  }

  @Retryable(
      retryFor = { FeignException.class },
      backoff = @Backoff(delay = 2000, multiplier = 2)
  )
  private SchemaMetadataResponse metadataTopicResponses(String baseUrl,
                            @MonotonicNonNull SchemaRegistryAuth authentication,
                            String tag) {
    try {
      String authorization = ConfluentAuthConfig.generateBasicAuthentication(authentication.username(),
          authentication.password());
      ResponseEntity<SchemaMetadataResponse> metadata = null;

      if (tag == null) {
        metadata = confluentApiClient.retrieveTopicMetadata(URI.create(baseUrl),
            authorization);
      } else {
        metadata = confluentApiClient.retrieveTopicMetadata(URI.create(baseUrl),
            authorization, tag);
      }
      if (metadata != null && metadata.getStatusCode().is2xxSuccessful()) {
        return metadata.getBody();
      }
    } catch (FeignException e) {
      log.error("Feign API call error with message: {}", e.getMessage());

    }
    return null;
  }

  @Retryable(
      retryFor = { FeignException.class },
      backoff = @Backoff(delay = 2000, multiplier = 2)
  )
  private SubjectMetadataResponse retrieveSubjectMetadataResponses(String baseUrl,
                                @MonotonicNonNull SchemaRegistryAuth authentication,
                                String topic) {
    try {
      String authorization = ConfluentAuthConfig.generateBasicAuthentication(authentication.username(),
          authentication.password());
      ResponseEntity<SubjectMetadataResponse> metadata =
          confluentApiClient.retrieveSubjectMetadata(URI.create(baseUrl), authorization, topic);
      if (metadata != null && metadata.getStatusCode().is2xxSuccessful()) {
        return metadata.getBody();
      }
    } catch (FeignException e) {
      log.error("Feign API call error with message: {}", e.getMessage());
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
      dynamoMaskingEntityRepository.save(mask);
    } catch (Exception e) {
      log.error("Dynamo Masking config failed with message: {}", e.getMessage());
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
      return new SchemaRegistryAuth(clusterAuth.getUsername(), clusterAuth.getPassword());
    } else {
      return extractSchemaRegistryAuth(source.getProperties().getProperty("sasl.jaas.config"));
    }
  }

  public static SchemaRegistryAuth extractSchemaRegistryAuth(String jaasConfig) {
    String clientId = extract(CLIENT_ID_PATTERN, jaasConfig);
    String clientSecret = extract(CLIENT_SECRET_PATTERN, jaasConfig);

    return new SchemaRegistryAuth(clientId, clientSecret);
  }

  private static String extract(Pattern pattern, String value) {
    Matcher matcher = pattern.matcher(value);
    if (matcher.find()) {
      return matcher.group(1);
    }

    throw new IllegalArgumentException(
        "Could not find " + pattern.pattern() + " in JAAS config");
  }
}
