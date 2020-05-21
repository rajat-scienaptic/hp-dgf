package com.hp.dgf.exception;

import com.hp.dgf.dto.response.ApiResponseDTO;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice
public class GlobalExceptionHandlerController {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    public void handleCustomException(HttpServletResponse res, CustomException e) throws IOException {
        LOG.error("ERROR", e);
        res.sendError(e.getHttpStatus().value(), e.getMessage());
    }

    @ExceptionHandler(AccessDeniedException.class)
    public void handleAccessDeniedException(HttpServletResponse res, AccessDeniedException e) throws IOException {
        LOG.error("ERROR", e);
        res.sendError(HttpStatus.FORBIDDEN.value(), "Access denied");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public void handleIllegalArgumentException(HttpServletResponse res, IllegalArgumentException e) throws IOException {
        LOG.error("ERROR", e);
        res.sendError(BAD_REQUEST.value(), "Something went wrong");
    }

    @ExceptionHandler(Exception.class)
    public void handleException(HttpServletResponse res, Exception e) throws IOException {
        LOG.error("ERROR", e);
        res.sendError(BAD_REQUEST.value(), "Something went wrong");
    }

    @ExceptionHandler(CustomException.class)
    public ResponseEntity<ApiResponseDTO> handleCustomException(CustomException e) {
        LOG.error(e.getMessage());
        return new ResponseEntity<>(ApiResponseDTO.builder()
                .timestamp(LocalDateTime.now())
                .status(e.getHttpStatus().value())
                .message(e.getMessage())
                .build(), e.getHttpStatus());
    }

    @ResponseStatus(BAD_REQUEST)
    @ResponseBody
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public Error methodArgumentNotValidException(MethodArgumentNotValidException ex) {
        BindingResult result = ex.getBindingResult();
        List<org.springframework.validation.FieldError> fieldErrors = result.getFieldErrors();
        return processFieldErrors(fieldErrors);
    }

    private Error processFieldErrors(List<org.springframework.validation.FieldError> fieldErrors) {
        Error error = new Error(BAD_REQUEST.value(), "validation error");
        for (org.springframework.validation.FieldError fieldError: fieldErrors) {
            error.addFieldError(Objects.requireNonNull(fieldError.getRejectedValue()).getClass().getName(), fieldError.getField(), fieldError.getDefaultMessage());
        }
        return error;
    }

    @Data
    static class Error {
        private final int status;
        private final String message;
        private List<FieldError> fieldErrors = new ArrayList<>();

        public void addFieldError(String type, String path, String message) {
            FieldError error = new FieldError(type, path, message);
            fieldErrors.add(error);
        }

        public List<FieldError> getFieldErrors() {
            return fieldErrors;
        }
    }

    @Data
    @AllArgsConstructor
    static class FieldError{
        String type;
        String path;
        String message;
    }
}