package com.hp.dgf.repository;

import com.hp.dgf.model.DGFSubGroupLevel1;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Transactional(readOnly = true)
@Repository
public interface DGFSubGroupLevel1Repository extends JpaRepository<DGFSubGroupLevel1, Integer> {
    @Query(value = "select d from DGFSubGroupLevel1 d where d.dgfGroupsId = :dgfGroupsId and d.baseRate = :name")
    DGFSubGroupLevel1 getDGFSubGroupLevel1(@Param("dgfGroupsId") int dgfGroupsId, @Param ("name") String name);
}
