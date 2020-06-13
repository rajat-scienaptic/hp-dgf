package com.hp.dgf.repository;

import com.hp.dgf.model.DGFSubGroupLevel3;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface DGFSubGroupLevel3Repository extends JpaRepository<DGFSubGroupLevel3, Integer> {
    @Query(value = "select d from DGFSubGroupLevel3 d where d.dgfSubGroupLevel2Id = :dgfSubGroupLevel2Id and d.baseRate = :name")
    DGFSubGroupLevel3 getDGFSubGroupLevel3(@Param("dgfSubGroupLevel2Id") int dgfSubGroupLevel2Id, @Param ("name") String name);

    @Query(value = "select d.dgfSubGroupLevel2Id from DGFSubGroupLevel3 d where d.id = :id")
    int getDgfSubGroupLevel2Id(@Param("id") int id);

    @Query(value = "select d.id from DGFSubGroupLevel3 d where d.dgfSubGroupLevel2Id = :dgfSubGroupLevel2Id")
    List<Integer> getDgfSubGroupLevel3IdList(@Param("dgfSubGroupLevel2Id") int dgfSubGroupLevel2Id);
}
