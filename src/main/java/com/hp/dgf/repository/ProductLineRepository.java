package com.hp.dgf.repository;

import com.hp.dgf.model.ProductLine;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@Repository
public interface ProductLineRepository extends JpaRepository<ProductLine, Integer> {
}
