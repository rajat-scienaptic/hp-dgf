package com.hp.dgf.repository;

import com.hp.dgf.model.DGFSubGroupLevel3;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface DGFSubGroupLevel3Repository extends JpaRepository<DGFSubGroupLevel3, Integer> {
    @Query(value = "select d from DGFSubGroupLevel3 d where d.dgfSubGroupLevel2Id = :dgfSubGroupLevel2Id and d.baseRate = :name")
    DGFSubGroupLevel3 getDGFSubGroupLevel3(@Param("dgfSubGroupLevel2Id") int dgfSubGroupLevel2Id, @Param ("name") String name);
}
