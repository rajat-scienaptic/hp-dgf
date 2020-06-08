package com.hp.dgf.repository;

import com.hp.dgf.model.BusinessSubCategory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface BusinessSubCategoryRepository extends JpaRepository<BusinessSubCategory, Integer> {
    @Query(value = "select b from BusinessSubCategory b where b.businessCategoryId = :id")
    List<BusinessSubCategory> getBusinessSubCategoriesPerBusinessCategoryId(@Param ("id") int id);
}
