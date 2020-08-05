package com.hp.dgf.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.dgf.exception.CustomException;
import com.hp.dgf.model.User;
import com.hp.dgf.service.UserValidationService;
import org.apache.tomcat.util.codec.binary.Base64;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class UserValidationServiceImpl implements UserValidationService {
    @Override
    public User getUserService(String userName, String email) {
        try {
            User user = new User();
            user.setName(userName);
            user.setEmail(email);
            user.setAuthenticated("true");
            return user;
        } catch (Exception e) {
            throw new CustomException("User Not Authorized !", HttpStatus.UNAUTHORIZED);
        }
    }

    @Override
    public String getUserNameFromCookie(String cookie) {
        try {
            Base64 base64Url = new Base64(true);
            String header = new String(base64Url.decode(cookie.split("\\|")[2]));
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readValue(header, JsonNode.class);
            return jsonNode.get("id_token").get("user_name").textValue();
        } catch (Exception e) {
            throw new CustomException("User Not Authorized !", HttpStatus.UNAUTHORIZED);
        }
    }
}
