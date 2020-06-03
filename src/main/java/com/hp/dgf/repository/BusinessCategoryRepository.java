package com.hp.dgf.repository;

import com.hp.dgf.model.BusinessCategory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Transactional(readOnly = true)
@Repository
public interface BusinessCategoryRepository extends JpaRepository<BusinessCategory, Integer> {
    @Query(value = "select b.name from BusinessCategory b")
    List<String> getBusinessCategories();
}
