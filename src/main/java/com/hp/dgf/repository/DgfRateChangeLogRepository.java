package com.hp.dgf.repository;

import com.hp.dgf.model.DGFRateChangeLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Transactional(readOnly = true)
@Repository
public interface DgfRateChangeLogRepository extends JpaRepository<DGFRateChangeLog, Integer> {
    @Query(value = "select d from DGFRateChangeLog d where d.dgfRateEntryId = :dgfRateEntryId")
    List<DGFRateChangeLog> getDGFRateChangeLogByRateEntryId(@Param("dgfRateEntryId") int dgfRateEntryId);
}
