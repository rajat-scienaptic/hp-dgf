package com.hp.dgf.exception;

import org.springframework.http.HttpStatus;

public final class CustomException extends RuntimeException{
    private static final long serialVersionUID = 1L;

    private final String message;
    private final Object body;
    private final HttpStatus httpStatus;

    public CustomException(String message, HttpStatus httpStatus) {
        this.message = message;
        this.httpStatus = httpStatus;
        body = null;
    }

    public CustomException(String message, Object body, HttpStatus httpStatus) {
        this.message = message;
        this.body = body;
        this.httpStatus = httpStatus;
    }

    @Override
    public final String getMessage() {
        return message;
    }

    public Object getBody() {
        return body;
    }

    public final HttpStatus getHttpStatus() {
        return httpStatus;
    }
}