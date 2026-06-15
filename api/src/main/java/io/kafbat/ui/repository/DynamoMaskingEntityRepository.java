package io.kafbat.ui.repository;

import io.kafbat.ui.model.sainsburys.dynamo.DynamoMaskingEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DynamoMaskingEntityRepository extends CrudRepository<DynamoMaskingEntity, String> {
}
