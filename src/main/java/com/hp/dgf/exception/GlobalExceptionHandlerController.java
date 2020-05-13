package com.hp.dgf.exception;

import com.hp.dgf.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.AccessDeniedException;

@RestControllerAdvice
public class GlobalExceptionHandlerController {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    @ExceptionHandler(CustomException.class)
    public void handleCustomException(HttpServletResponse res, CustomException e) throws IOException {
        LOG.error(Constants.ERROR, e);
        res.sendError(e.getHttpStatus().value(), e.getMessage());
    }

    @ExceptionHandler(AccessDeniedException.class)
    public void handleAccessDeniedException(HttpServletResponse res, AccessDeniedException e) throws IOException {
        LOG.error(Constants.ERROR, e);
        res.sendError(HttpStatus.FORBIDDEN.value(), Constants.ACCESS_DENIED);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public void handleIllegalArgumentException(HttpServletResponse res, IllegalArgumentException e) throws IOException {
        LOG.error(Constants.ERROR, e);
        res.sendError(HttpStatus.BAD_REQUEST.value(), Constants.SOMETHING_WENT_WRONG);
    }

    @ExceptionHandler(Exception.class)
    public void handleException(HttpServletResponse res, Exception e) throws IOException {
        LOG.error(Constants.ERROR, e);
        res.sendError(HttpStatus.BAD_REQUEST.value(), Constants.SOMETHING_WENT_WRONG);
    }


}