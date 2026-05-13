package io.kafbat.ui.repository;

import io.kafbat.ui.model.sainsburys.dynamo.DynamoMaskingEntity;
import java.util.List;
import org.socialsignin.spring.data.dynamodb.repository.EnableScan;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@EnableScan
@Repository
public interface DynamoMaskingEntityRepository extends CrudRepository<DynamoMaskingEntity, String> {
  List<DynamoMaskingEntity> findAll();
}
