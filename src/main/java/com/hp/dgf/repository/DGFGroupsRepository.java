package com.hp.dgf.repository;

import com.hp.dgf.model.DGFGroups;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Transactional(readOnly = true)
@Repository
public interface DGFGroupsRepository extends JpaRepository<DGFGroups, Integer> {
    @Query(value = "select d from DGFGroups d where d.businessCategoryId = :businessCategoryId and d.baseRate = :name")
    DGFGroups getDGFGroup(@Param ("businessCategoryId") int businessCategoryId, @Param ("name") String name);
}
