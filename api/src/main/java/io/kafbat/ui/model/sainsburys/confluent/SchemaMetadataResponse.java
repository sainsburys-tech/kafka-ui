package io.kafbat.ui.model.sainsburys.confluent;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.Valid;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.lang.Nullable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaMetadataResponse {

  private @Nullable SearchParameters searchParameters;

  @Valid
  private List<String> types = new ArrayList<>();

  @Valid
  private List<@Valid Entity> entities = new ArrayList<>();
}
