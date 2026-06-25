package io.kafbat.ui.controller.sainsburys;

import static io.kafbat.ui.model.rbac.permission.TopicAction.MESSAGES_READ;

import io.kafbat.ui.api.UnmaskingApi;
import io.kafbat.ui.controller.AbstractController;
import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.UnmaskRequestDTO;
import io.kafbat.ui.model.rbac.AccessContext;
import io.kafbat.ui.service.mcp.McpTool;
import io.kafbat.ui.service.sainsburys.UnmaskingService;
import java.security.Principal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@Slf4j
public class UnmaskingController extends AbstractController implements UnmaskingApi, McpTool {

  private final UnmaskingService unmaskingService;

  @Value("${sainsburys.masking.feature.enabled: 'false' }")
  private String isMaskingEnabled;

  @Override
  public Mono<ResponseEntity<Void>> dataUnmasking(String clusterName, String topicName,
                                                  Mono<UnmaskRequestDTO> unmaskRequestDto, ServerWebExchange exchange) {
    log.info("Unmasking cluster {}, topic {} messages", clusterName, topicName);

    if (!Boolean.parseBoolean(isMaskingEnabled)) {
      throw new ValidationException("Data masking feature is not in active status.");
    }

    return exchange.getPrincipal()
        .map(Principal::getName)
        .flatMap(principal -> {
          var context = AccessContext.builder()
              .cluster(clusterName)
              .topicActions(topicName, MESSAGES_READ)
              .operationName("getTopicMessages")
              .build();
          return validateAccess(context).then(
              unmaskRequestDto.flatMap(msg ->
                  unmaskingService.decrypt(getCluster(clusterName), topicName, msg, principal)
              ).map(m -> new ResponseEntity<Void>(HttpStatus.OK))
          ).doOnEach(sig -> audit(context, sig));
        });
  }
}
