package io.kafbat.ui.model.sainsburys.servicenow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
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
  private String uaAssignedTo;

  @JsonProperty("u_assignment_group")
  private String uaAssignmentGroup;

  @JsonProperty("u_business_service")
  private String ubBusinessService;

  @JsonProperty("u_caller_id")
  private String ucCallerId;

  @JsonProperty("u_category")
  private String ucCategory;

  @JsonProperty("u_subcategory")
  private String usSubcategory;

  @JsonProperty("u_cmdb_ci")
  private String ucCmdbCi;

  @JsonProperty("u_comments")
  private String ucComments;

  @JsonProperty("u_description")
  private String udDescription;

  @JsonProperty("u_impact")
  private int uiImpact;

  @JsonProperty("u_urgency")
  private int uuUrgency;

  @JsonProperty("u_impacted_parties")
  private String uiImpactedParties;

  @JsonProperty("u_location_not_found")
  private String ulLocationNotFound;

  @JsonProperty("u_undefined_location")
  private String uuUndefinedLocation;

  @JsonProperty("u_short_description")
  private String usShortDescription;

  @JsonProperty("u_state")
  private int usState;

  @JsonProperty("u_work_notes")
  private String uwWorkNotes;
}
