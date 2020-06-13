package com.hp.dgf.repository;

import com.hp.dgf.model.DGFRateChangeLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Transactional(readOnly = true)
@Repository
public interface DGFRateChangeLogRepository extends JpaRepository<DGFRateChangeLog, Integer> {
    @Query(value = "select d from DGFRateChangeLog d where d.dgfRateEntryId = :dgfRateEntryId")
    List<DGFRateChangeLog> getDGFRateChangeLogByRateEntryId(@Param("dgfRateEntryId") int dgfRateEntryId);

    @Query(value = "SELECT dgf_rate FROM " +
            "dgf_sandbox.dgf_rate_change_log s1 where created_on = (SELECT MAX(created_on) " +
            "FROM dgf_sandbox.dgf_rate_change_log s2 WHERE " +
            "s1.dgf_rate_entry_id = s2.dgf_rate_entry_id and created_on <= :createdOn " +
            "and dgf_rate_entry_id = :id) " +
            "ORDER BY dgf_rate_entry_id, created_on", nativeQuery = true)
    BigDecimal getLatestData(@Param("createdOn") LocalDateTime createdOn
            , @Param("id") int id);

//    @Query(value = "SELECT dgf_rate FROM " +
//            "dgf.dgf_rate_change_log s1 where created_on = (SELECT MAX(created_on) " +
//            "FROM dgf.dgf_rate_change_log s2 WHERE " +
//            "s1.dgf_rate_entry_id = s2.dgf_rate_entry_id and created_on <= :createdOn " +
//            "and dgf_rate_entry_id = :id) " +
//            "ORDER BY dgf_rate_entry_id, created_on", nativeQuery = true)
//    BigDecimal getLatestData(@Param("createdOn") LocalDateTime createdOn
//            , @Param("id") int id);
}
