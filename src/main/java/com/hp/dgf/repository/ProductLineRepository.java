package com.hp.dgf.repository;

import com.hp.dgf.model.ProductLine;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProductLineRepository extends JpaRepository<ProductLine, Integer> {
    @Query(value = "select p from ProductLine p where p.code = :code")
    ProductLine checkIfPlExists(@Param ("code") String code);

    @Query(value = "select p.businessSubCategoryId from ProductLine p where p.code = :code")
    Integer getBusinessSubCategoryIdByPLId(@Param ("code") String code);

    @Query(value = "select p from ProductLine p where p.isActive = 1 and p.businessSubCategoryId = :id")
    List<ProductLine> getProductLinesPerBusinessSubCategory(@Param ("id") int id);
}
