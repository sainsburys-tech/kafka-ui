package io.kafbat.ui.repository;

import io.kafbat.ui.model.sainsburys.dynamo.DynamoRbacEntity;
import java.util.List;
import org.socialsignin.spring.data.dynamodb.repository.EnableScan;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@EnableScan
@Repository
public interface DynamoRbacEntityRepository extends CrudRepository<DynamoRbacEntity, String> {
  List<DynamoRbacEntity> findAll();
}
