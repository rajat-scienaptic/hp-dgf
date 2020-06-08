package com.hp.dgf.repository;

import com.hp.dgf.model.DGFRateEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Transactional
@Repository
public interface DGFRateEntryRepository extends JpaRepository<DGFRateEntry, Integer> {
    @Query(value = "select d.productLineId from DGFRateEntry d where d.dgfSubGroupLevel2Id = :dgfSubGroupLevel2Id")
    Integer getPLId(@Param("dgfSubGroupLevel2Id") int dgfSubGroupLevel2Id);

    @Query(value = "select d from DGFRateEntry d where d.dgfSubGroupLevel2Id = :dgfSubGroupLevel2Id and d.createdOn between :createdOn and :endDate")
    List<DGFRateEntry> findByDgfSubGroupLevel2IdAndCreatedOnBetween(
            @Param ("dgfSubGroupLevel2Id") int dgfSubGroupLevel2Id,
            @Param ("createdOn") LocalDateTime createdOn, @Param("endDate") LocalDateTime endDate);

    @Query(value = "select d from DGFRateEntry d where d.dgfSubGroupLevel3Id = :dgfSubGroupLevel3Id and d.createdOn between :createdOn and :endDate")
    List<DGFRateEntry> findByDgfSubGroupLevel3IdAndCreatedOnBetween(
            @Param ("dgfSubGroupLevel3Id") int dgfSubGroupLevel3Id,
            @Param ("createdOn") LocalDateTime createdOn, @Param("endDate") LocalDateTime endDate);

}
