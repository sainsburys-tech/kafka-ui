package io.kafbat.ui.model.sainsburys.servicenow;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceNowCreate implements Serializable {

  @JsonProperty("u_assigned_to")
  private String uAssignedTo;

  @JsonProperty("u_assignment_group")
  private String uAssignmentGroup;

  @JsonProperty("u_business_service")
  private String uBusinessService;

  @JsonProperty("u_caller_id")
  private String uCallerId;

  @JsonProperty("u_category")
  private String uCategory;

  @JsonProperty("u_subcategory")
  private String uSubcategory;

  @JsonProperty("u_cmdb_ci")
  private String uCmdbCi;

  @JsonProperty("u_comments")
  private String uComments;

  @JsonProperty("u_description")
  private String uDescription;

  @JsonProperty("u_impact")
  private int uImpact;

  @JsonProperty("u_urgency")
  private int uUrgency;

  @JsonProperty("u_impacted_parties")
  private String uImpactedParties;

  @JsonProperty("u_location_not_found")
  private String uLocationNotFound;

  @JsonProperty("u_undefined_location")
  private String uUndefinedLocation;

  @JsonProperty("u_short_description")
  private String uShortDescription;

  @JsonProperty("u_state")
  private int uState;

  @JsonProperty("u_work_notes")
  private String uWorkNotes;
}
