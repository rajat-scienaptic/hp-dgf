package com.hp.dgf.utils;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class FileExtensionService {
    public String getFileExtension(MultipartFile file) {
        String fileName = file.getName();
        if(fileName.lastIndexOf('.') != -1 && fileName.lastIndexOf('.') != 0)
            return fileName.substring(fileName.lastIndexOf('.')+1);
        else return "";
    }
}
