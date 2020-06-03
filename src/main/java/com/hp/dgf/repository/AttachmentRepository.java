package com.hp.dgf.repository;

import com.hp.dgf.model.Attachment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Transactional(readOnly = true)
@Repository
public interface AttachmentRepository extends JpaRepository<Attachment, Integer> {
}
