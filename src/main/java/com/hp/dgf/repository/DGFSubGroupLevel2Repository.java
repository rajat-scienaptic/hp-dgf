package com.hp.dgf.repository;

import com.hp.dgf.model.DGFSubGroupLevel2;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Transactional(readOnly = true)
@Repository
public interface DGFSubGroupLevel2Repository extends JpaRepository<DGFSubGroupLevel2, Integer> {
    @Query(value = "select d from DGFSubGroupLevel2 d where d.dgfSubGroupLevel1Id = :dgfSubGroupLevel1Id and d.baseRate = :name")
    DGFSubGroupLevel2 getDGFSubGroupLevel2(@Param("dgfSubGroupLevel1Id") int dgfSubGroupLevel1Id, @Param ("name") String name);
}
