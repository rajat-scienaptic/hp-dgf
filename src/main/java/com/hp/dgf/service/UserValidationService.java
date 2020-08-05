package com.hp.dgf.service;

import com.hp.dgf.model.User;

public interface UserValidationService {
    public User getUserService(String userName, String email);
    public String getUserNameFromCookie(String cookie);
}
