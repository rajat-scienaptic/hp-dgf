package com.hp.dgf.service;

import org.springframework.core.io.Resource;
import org.springframework.web.multipart.MultipartFile;

public interface AttachmentService {
    String saveAttachment(MultipartFile file);
    Resource loadFileAsResource(String fileName);
}
