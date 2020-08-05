package com.hp.dgf.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;


@ResponseStatus(value = HttpStatus.UNAUTHORIZED, reason = "UNAUTHORIZED ACCESS")
public class UnAuthorizedAccessException extends RuntimeException {
    public UnAuthorizedAccessException() {

    }

    public UnAuthorizedAccessException(String exception) {
        super(exception);
    }
}
