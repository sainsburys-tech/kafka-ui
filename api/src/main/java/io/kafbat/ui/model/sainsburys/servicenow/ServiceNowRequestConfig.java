package io.kafbat.ui.model.sainsburys.servicenow;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@Configuration
@ConfigurationProperties(prefix = "sainsburys.external.services.service-now.requests.content")
public class ServiceNowRequestConfig implements Serializable {

  private String uAssignedTo;
  private String uAssignmentGroup;
  private String uBusinessService;
  private String uCallerId;
  private String uCategory;
  private String uSubcategory;
  private String uCmdbCi;
  private String uComments;
  private String uDescription;
  private int uImpact;
  private int uUrgency;
  private String uImpactedParties;
  private String uLocationNotFound;
  private String uUndefinedLocation;
  private String uShortDescription;
  private int uState;
  private String uWorkNotes;
}
