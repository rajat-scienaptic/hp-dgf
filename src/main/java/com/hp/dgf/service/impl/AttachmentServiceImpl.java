package com.hp.dgf.service.impl;

import com.hp.dgf.exception.CustomException;
import com.hp.dgf.model.FileStorageProperties;
import com.hp.dgf.repository.AttachmentRepository;
import com.hp.dgf.service.AttachmentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.Objects;

@Service
public class AttachmentServiceImpl implements AttachmentService {
    @Autowired
    private AttachmentRepository attachmentRepository;

    private static final Logger logger = LoggerFactory.getLogger(AttachmentServiceImpl.class);

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

    public String save(MultipartFile file) {
        // Normalize file name
        String fileName = StringUtils.cleanPath(Objects.requireNonNull(file.getOriginalFilename()));
        String uniqueFileName = fileName+"_"+LocalDateTime.now()+"."+getFileExtension(file);

        try {
            // Check if the file's name contains invalid characters
            if(file.getOriginalFilename().contains("..")) {
                throw new CustomException("Sorry! Filename contains invalid path sequence " + fileName, HttpStatus.BAD_REQUEST);
            }
            // Copy file to the target location (Replacing existing file with the same name)
            Path targetLocation = this.root.resolve(fileName);

            Files.copy(file.getInputStream(), targetLocation, StandardCopyOption.REPLACE_EXISTING);

            return ServletUriComponentsBuilder.fromCurrentContextPath()
                    .path("/"+root+"/")
                    .path(uniqueFileName)
                    .toUriString();
        } catch (IOException ex) {
            throw new CustomException("Could not store file " + fileName + ". Make sure the uploads directory exists !", HttpStatus.BAD_REQUEST);
        }
    }

    private Resource loadFileAsResource(String fileName) {
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

    private ResponseEntity<Resource> downloadFile(@PathVariable String fileName, HttpServletRequest request) {
        // Load file as Resource
        Resource resource = loadFileAsResource(fileName);

        // Try to determine file's content type
        String contentType = null;
        try {
            contentType = request.getServletContext().getMimeType(resource.getFile().getAbsolutePath());
        } catch (IOException ex) {
            logger.info("Could not determine file type.");
        }

        // Fallback to the default content type if type could not be determined
        if(contentType == null) {
            contentType = "application/octet-stream";
        }

        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(contentType))
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + resource.getFilename() + "\"")
                .body(resource);
    }

    private String getFileExtension(MultipartFile file) {
        String fileName = file.getName();
        if(fileName.lastIndexOf('.') != -1 && fileName.lastIndexOf('.') != 0)
            return fileName.substring(fileName.lastIndexOf('.')+1);
        else return "";
    }
}
