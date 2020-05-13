package com.hp.dgf.repository;

import com.hp.dgf.model.DGFGroups;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Transactional(readOnly = true)
@Repository
public interface DGFRepository extends JpaRepository<DGFGroups, Integer> {
}
