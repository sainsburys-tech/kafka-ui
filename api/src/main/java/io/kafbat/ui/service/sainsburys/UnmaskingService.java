package io.kafbat.ui.service.sainsburys;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import feign.FeignException;
import io.kafbat.ui.client.sainsburys.ServiceNowClient;
import io.kafbat.ui.exception.TopicNotFoundException;
import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.mapper.DynamicConfigMapper;
import io.kafbat.ui.model.*;
import io.kafbat.ui.model.rbac.provider.Provider;
import io.kafbat.ui.model.sainsburys.dynamo.DynamoPermission;
import io.kafbat.ui.model.sainsburys.dynamo.DynamoRbacEntity;
import io.kafbat.ui.model.sainsburys.dynamo.DynamoSubject;
import io.kafbat.ui.model.sainsburys.servicenow.ServiceNowCreate;
import io.kafbat.ui.model.sainsburys.servicenow.ServiceNowRequestConfig;
import io.kafbat.ui.repository.DynamoRbacEntityRepository;
import io.kafbat.ui.service.AdminClientService;
import io.kafbat.ui.util.DynamicConfigOperations;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
@EnableRetry
public class UnmaskingService {

  public static final String KAFKA_CLUSTER_SERVICENOW_DESCRIPTION = "<kafka_cluster>";
  public static final String KAFKA_TOPIC_SERVICENOW_DESCRIPTION = "<kafka_topic>";
  public static final String JUSTIFICATION_SERVICENOW_DESCRIPTION = "<justification>";
  public static final String RBAC_UNMASK_USER_ROLE_S_S_S_UNMASK = "%s_%s_%s_unmask";
  private final AdminClientService adminClientService;
  private final ServiceNowClient serviceNowClient;
  private final DynamicConfigOperations dynamicConfigOperations;
  private final DynamicConfigMapper configMapper;
  private final ServiceNowRequestConfig serviceNowRequestConfig;
  private final DynamoRbacEntityRepository dynamoRbacEntityRepository;

  @Value("${sainsburys.masking.rule.time-to-live: 3600000}")
  private Long timeToLive;

  @Value("${sainsburys.masking.date.format: yyyy-MM-dd HH:mm:ss.SSS }")
  private String dateFormat;

  @Value("${sainsburys.masking.subject.type: user }")
  private String subjectType;

  @Value("${sainsburys.masking.cluster.unmasked-prefix:unmasked-}")
  private String unmaskedClusterPrefix;

  public UnmaskingService(AdminClientService adminClientService, ServiceNowClient serviceNowClient,
                          DynamicConfigOperations dynamicConfigOperations, DynamicConfigMapper configMapper,
                          ServiceNowRequestConfig serviceNowRequestConfig,
                          DynamoRbacEntityRepository dynamoRbacEntityRepository) {
    this.adminClientService = adminClientService;
    this.serviceNowClient = serviceNowClient;
    this.dynamicConfigOperations = dynamicConfigOperations;
    this.configMapper = configMapper;
    this.serviceNowRequestConfig = serviceNowRequestConfig;
    this.dynamoRbacEntityRepository = dynamoRbacEntityRepository;
  }

  private Mono<TopicDescription> withExistingTopic(KafkaCluster cluster, String topicName) {
    return adminClientService.get(cluster)
        .flatMap(client -> client.describeTopic(topicName))
        .switchIfEmpty(Mono.error(new TopicNotFoundException()));
  }

  public Mono<RecordMetadata> decrypt(KafkaCluster cluster, String topic,
                                      UnmaskRequestDTO msg, String principal) {
    log.info("Unmasking Request: {}", msg.getJustification());
    return withExistingTopic(cluster, topic)
        .publishOn(Schedulers.boundedElastic())
        .flatMap(desc -> decryptImpl(cluster, desc, msg, principal));
  }

  private Mono<RecordMetadata> decryptImpl(KafkaCluster cluster,
                                           TopicDescription topicDescription,
                                           UnmaskRequestDTO msg, String principal) {
    if (msg.getJustification() == null) {
      return Mono.error(new ValidationException("No justification provided for request"));
    }

    try {
      boolean isAuditCreated = logServiceNowTicket(cluster.getName(),
          topicDescription.name(),
          msg.getJustification(),
          principal);
      if (isAuditCreated) {
        ApplicationConfigPropertiesDTO config = configMapper.toDto(dynamicConfigOperations.getCurrentProperties());
        AtomicBoolean isRoleAssigned = new AtomicBoolean(false);
        updateRbacConfig(cluster.getName(), topicDescription.name(), principal, config, isRoleAssigned);
        log.info("Persist cluster config change");
        config.getRbac().getRoles().forEach(r-> log.info("RBAC Role: {}", r.getName()));
      }
      return Mono.empty();
    } catch (Throwable e) {
      return Mono.error(e);
    }
  }


  @Retryable(
      retryFor = { FeignException.class },
      backoff = @Backoff(delay = 2000, multiplier = 2)
  )
  private boolean logServiceNowTicket(String cluster, String topic, String justification, String username) {
    try {
      ServiceNowCreate payload = buildServiceNowCreatePayload(cluster, topic, justification, username);
      ResponseEntity<Object> response = serviceNowClient.createAuditTicket( payload);
      return response != null && response.getStatusCode().is2xxSuccessful();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void updateRbacConfig(String cluster, String topic, String principal, ApplicationConfigPropertiesDTO config,
                                AtomicBoolean isRoleAssigned) {
    String unmaskPrincipalRole = String.format(RBAC_UNMASK_USER_ROLE_S_S_S_UNMASK, cluster, topic, principal);
    List<@Valid ApplicationConfigPropertiesRbacRolesInnerDTO> configPropertiesRbacRolesInnerDTOList = config.getRbac().getRoles();

    boolean isUnmaskRoleNotExist = configPropertiesRbacRolesInnerDTOList.stream()
        .filter(r -> r.getName().contains(unmaskPrincipalRole))
        .toList()
        .isEmpty();

    if (isUnmaskRoleNotExist) {
      ApplicationConfigPropertiesRbacRolesInnerDTO rbacRolesInnerDTO = new ApplicationConfigPropertiesRbacRolesInnerDTO();
      rbacRolesInnerDTO.setName(unmaskPrincipalRole);
      rbacRolesInnerDTO.setClusters(List.of(unmaskedClusterPrefix + cluster));

      ApplicationConfigPropertiesRbacRolesInnerSubjectsInnerDTO rbacRolesInnerSubjectsInnerDTO =
          getApplicationConfigPropertiesRbacRolesInnerSubjectsInnerDTO(cluster, principal, config);

      rbacRolesInnerDTO.setSubjects(List.of(rbacRolesInnerSubjectsInnerDTO));

      RbacPermissionDTO clusterPermissionsInnerDTO = new RbacPermissionDTO();
      clusterPermissionsInnerDTO.setResource(ResourceTypeDTO.CLUSTERCONFIG);
      clusterPermissionsInnerDTO.setActions(List.of(ActionDTO.VIEW));

      rbacRolesInnerDTO.addPermissionsItem(clusterPermissionsInnerDTO);

      RbacPermissionDTO topicPermissionsInnerDTO = new RbacPermissionDTO();
      topicPermissionsInnerDTO.setResource(ResourceTypeDTO.TOPIC);
      topicPermissionsInnerDTO.setActions(Arrays.asList(ActionDTO.VIEW, ActionDTO.MESSAGES_READ));
      topicPermissionsInnerDTO.setValue(topic);

      rbacRolesInnerDTO.addPermissionsItem(topicPermissionsInnerDTO);
      config.getRbac().addRolesItem(rbacRolesInnerDTO);
      isRoleAssigned.set(true);
      createDynamoRbac(mapperFromRbacRoleDto(rbacRolesInnerDTO));
    } else {
      throw new ValidationException("Data unmask role already assigned");
    }
  }

  private @NonNull ApplicationConfigPropertiesRbacRolesInnerSubjectsInnerDTO getApplicationConfigPropertiesRbacRolesInnerSubjectsInnerDTO(String cluster,
                                                                                                                                          String principal, ApplicationConfigPropertiesDTO config) {
    ApplicationConfigPropertiesRbacRolesInnerSubjectsInnerDTO rbacRolesInnerSubjectsInnerDTO = new ApplicationConfigPropertiesRbacRolesInnerSubjectsInnerDTO();

    assingPrincipalAuthProvider(cluster, principal, config, rbacRolesInnerSubjectsInnerDTO);

    rbacRolesInnerSubjectsInnerDTO.setValue(principal);
    rbacRolesInnerSubjectsInnerDTO.setType(subjectType);

    return rbacRolesInnerSubjectsInnerDTO;
  }

  private ServiceNowCreate buildServiceNowCreatePayload(String cluster, String topic, String justification, String username) {

    String description = serviceNowRequestConfig.getUDescription().replace(KAFKA_CLUSTER_SERVICENOW_DESCRIPTION, cluster + "\n");
    description = description.replace(KAFKA_TOPIC_SERVICENOW_DESCRIPTION, topic + "\n");
    description = description.replace(JUSTIFICATION_SERVICENOW_DESCRIPTION, justification);

    return ServiceNowCreate.builder()
        .uAssignedTo(serviceNowRequestConfig.getUAssignedTo())
        .uAssignmentGroup(serviceNowRequestConfig.getUAssignmentGroup())
        .uBusinessService(serviceNowRequestConfig.getUBusinessService())
        .uCallerId(username)
        .uCategory(serviceNowRequestConfig.getUCategory())
        .uSubcategory(serviceNowRequestConfig.getUSubcategory())
        .uCmdbCi(serviceNowRequestConfig.getUCmdbCi())
        .uComments(serviceNowRequestConfig.getUComments())
        .uDescription(description)
        .uImpact(serviceNowRequestConfig.getUImpact())
        .uUrgency(serviceNowRequestConfig.getUUrgency())
        .uImpactedParties(serviceNowRequestConfig.getUImpactedParties())
        .uLocationNotFound(serviceNowRequestConfig.getULocationNotFound())
        .uUndefinedLocation(serviceNowRequestConfig.getUUndefinedLocation())
        .uShortDescription(serviceNowRequestConfig.getUShortDescription())
        .uState(serviceNowRequestConfig.getUState())
        .uWorkNotes(serviceNowRequestConfig.getUWorkNotes())
        .build();
  }

  private static void assingPrincipalAuthProvider(String cluster, String principal, ApplicationConfigPropertiesDTO config,
                                                  ApplicationConfigPropertiesRbacRolesInnerSubjectsInnerDTO rbacRolesInnerSubjectsInnerDTO) {
    config.getRbac().getRoles().stream()
        .filter(r -> r.getClusters().contains(cluster))
        .forEach(role -> {
          role.getSubjects().stream()
              .filter(s -> s.getValue().equalsIgnoreCase(principal))
              .findFirst()
              .ifPresent(principalRole -> rbacRolesInnerSubjectsInnerDTO.setProvider(principalRole.getProvider()));
        });
  }

  @Retryable(
      retryFor = {
          ProvisionedThroughputExceededException.class,
          SdkClientException.class
      },
      backoff = @Backoff(delay = 500, multiplier = 2)
  )
  private void createDynamoRbac(DynamoRbacEntity rbac) {
    try {
      dynamoRbacEntityRepository.save(rbac);
    } catch (Exception e) {
      log.error("RBAC dynamic config dynamo persist failed with message: {}", e.getMessage());
    }
  }

  private DynamoRbacEntity mapperFromRbacRoleDto(ApplicationConfigPropertiesRbacRolesInnerDTO source) {
    var subjects = source.getSubjects().stream().map(this::mapperFromSubjectsDto).toList();
    return DynamoRbacEntity.builder()
        .name(source.getName())
        .clusters(source.getClusters())
        .subjects(subjects)
        .permissions(source.getPermissions().stream()
            .map(this::mapperFromPermissionsDto).toList())
        .expireTime(getLongDateFromStr(subjects.stream().findFirst().map(DynamoSubject::getExpiryTime).get()))
        .build();
  }

  private DynamoSubject mapperFromSubjectsDto(ApplicationConfigPropertiesRbacRolesInnerSubjectsInnerDTO source) {
    DynamoSubject target = new DynamoSubject();
    target.setType(source.getType());
    target.setValue(source.getValue());
    target.setProvider(Provider.valueOf(source.getProvider()));
    target.setRegex(false);

    Calendar calendar = Calendar.getInstance();
    Date currentDate = calendar.getTime();

    SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
    String createdTime = sdf.format(currentDate);
    target.setCreatedTime(createdTime);


    calendar.add(Calendar.MILLISECOND, timeToLive.intValue());
    Date expiryTime = calendar.getTime();
    String expiryTimeStr = sdf.format(expiryTime);
    target.setExpiryTime(expiryTimeStr);

    return target;
  }

  private DynamoPermission mapperFromPermissionsDto(RbacPermissionDTO source) {
    DynamoPermission target = new DynamoPermission();
    target.setValue(source.getValue());
    target.setActions(source.getActions().stream().map(ActionDTO::getValue).toList());
    target.setResource(source.getResource().getValue());
    return target;
  }

  private Long getLongDateFromStr(String date) {
    SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
    try {
      Date expiryDate = sdf.parse(date);
      return expiryDate.getTime();
    } catch (ParseException e) {
      log.error("Time to live failed on: {}", e.getMessage());
    }
    return null;
  }

}
