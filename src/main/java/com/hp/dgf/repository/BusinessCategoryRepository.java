package com.hp.dgf.repository;

import com.hp.dgf.model.BusinessCategory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Transactional(readOnly = true)
@Repository
public interface BusinessCategoryRepository extends JpaRepository<BusinessCategory, Integer> {
    @Query(value = "select b from BusinessCategory b where b.id = :id")
    BusinessCategory getBusinessCategoryData(@Param ("id") int id);
}
