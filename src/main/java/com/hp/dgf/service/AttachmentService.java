package com.hp.dgf.service;

import org.springframework.web.multipart.MultipartFile;

public interface AttachmentService {
    String save(MultipartFile file);
}
