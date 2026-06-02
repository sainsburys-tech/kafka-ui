package io.kafbat.ui.model.sainsburys.servicenow;

import java.io.Serializable;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.Name;

@Getter
@ConfigurationProperties(prefix = "sainsburys.external.services.service-now.requests.content")
public class ServiceNowRequestConfig implements Serializable {

  private final String uaAssignedTo;
  private final String uaAssignmentGroup;
  private final String ubBusinessService;
  private final String ucCallerId;
  private final String ucCategory;
  private final String usSubcategory;
  private final String ucCmdbCi;
  private final String ucComments;
  private final String udDescription;
  private final int uiImpact;
  private final int uuUrgency;
  private final String uiImpactedParties;
  private final String ulLocationNotFound;
  private final String uuUndefinedLocation;
  private final String usShortDescription;
  private final int usState;
  private final String uwWorkNotes;

  public ServiceNowRequestConfig(
      @Name("u_assigned_to") String uaAssignedTo,
      @Name("u_assignment_group") String uaAssignmentGroup,
      @Name("u_business_service") String ubBusinessService,
      @Name("u_caller_id") String ucCallerId,
      @Name("u_category") String ucCategory,
      @Name("u_subcategory") String usSubcategory,
      @Name("u_cmdb_ci") String ucCmdbCi,
      @Name("u_comments") String ucComments,
      @Name("u_description") String udDescription,
      @Name("u_impact") int uiImpact,
      @Name("u_urgency") int uuUrgency,
      @Name("u_impacted_parties") String uiImpactedParties,
      @Name("u_location_not_found") String ulLocationNotFound,
      @Name("u_undefined_location") String uuUndefinedLocation,
      @Name("u_short_description") String usShortDescription,
      @Name("u_state") int usState,
      @Name("u_work_notes") String uwWorkNotes) {

    this.uaAssignedTo = uaAssignedTo;
    this.uaAssignmentGroup = uaAssignmentGroup;
    this.ubBusinessService = ubBusinessService;
    this.ucCallerId = ucCallerId;
    this.ucCategory = ucCategory;
    this.usSubcategory = usSubcategory;
    this.ucCmdbCi = ucCmdbCi;
    this.ucComments = ucComments;
    this.udDescription = udDescription;
    this.uiImpact = uiImpact;
    this.uuUrgency = uuUrgency;
    this.uiImpactedParties = uiImpactedParties;
    this.ulLocationNotFound = ulLocationNotFound;
    this.uuUndefinedLocation = uuUndefinedLocation;
    this.usShortDescription = usShortDescription;
    this.usState = usState;
    this.uwWorkNotes = uwWorkNotes;
  }
}
