package com.hp.dgf.repository;

import com.hp.dgf.model.DGFLogs;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DGFLogRepository extends JpaRepository<DGFLogs, Integer> {
}
