package com.hp.dgf.repository;

import com.hp.dgf.model.ColorCode;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Transactional(readOnly = true)
@Repository
public interface ColorCodeRepository extends JpaRepository<ColorCode, Integer> {
    @Query(value = "select c from ColorCode c where c.fyQuarter = :fyQuarter")
    ColorCode getColorCodeByFyQuarter(@Param("fyQuarter") String fyQuarter);
}
