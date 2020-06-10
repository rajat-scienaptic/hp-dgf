package com.hp.dgf.service.impl;

import com.hp.dgf.exception.CustomException;
import com.hp.dgf.model.FileStorageProperties;
import com.hp.dgf.repository.AttachmentRepository;
import com.hp.dgf.service.AttachmentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

@Service
public class AttachmentServiceImpl implements AttachmentService {
    @Autowired
    private AttachmentRepository attachmentRepository;

    private final Path root;

    @Autowired
    public AttachmentServiceImpl(FileStorageProperties fileStorageProperties) {
        this.root = Paths.get(fileStorageProperties.getUploadDir())
                .toAbsolutePath().normalize();
        try {
            Files.createDirectories(this.root);
        } catch (Exception ex) {
            throw new CustomException("Could not create the directory where the uploaded files will be stored.", HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public String saveAttachment(MultipartFile file) {
        // Normalize file name
        // Normalize file name
        String[] fileName = StringUtils.cleanPath(Objects.requireNonNull(file.getOriginalFilename())).split("\\.");
        String uniqueFileName = fileName[0]+"_"+new SimpleDateFormat("yyyyMMddHHmm").format(new Date())+"."+fileName[1];

        try {
            // Check if the file's name contains invalid characters
            if(file.getOriginalFilename().contains("..")) {
                throw new CustomException("Sorry! Filename contains invalid path sequence " + uniqueFileName, HttpStatus.BAD_REQUEST);
            }
            // Copy file to the target location (Replacing existing file with the same name)
            Path targetLocation = this.root.resolve(uniqueFileName);

            Files.copy(file.getInputStream(), targetLocation, StandardCopyOption.REPLACE_EXISTING);

            ServletUriComponentsBuilder.fromCurrentContextPath()
                    .path("/"+root+"/")
                    .path(uniqueFileName)
                    .toUriString();

            return uniqueFileName;
        } catch (IOException ex) {
            throw new CustomException("Could not store file " + uniqueFileName + ". Make sure the uploads directory exists !", HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public Resource loadFileAsResource(String fileName) {
        try {
            Path filePath = this.root.resolve(fileName).normalize();
            Resource resource = new UrlResource(filePath.toUri());
            if(resource.exists()) {
                return resource;
            } else {
                throw new CustomException("File not found " + fileName, HttpStatus.NOT_FOUND);
            }
        } catch (MalformedURLException ex) {
            throw new CustomException("File not found " + fileName, HttpStatus.NOT_FOUND);
        }
    }
}
