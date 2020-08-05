package com.hp.dgf.controller;

import com.hp.dgf.model.User;
import com.hp.dgf.service.UserValidationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

//@CrossOrigin(origins = "*")
@RequestMapping("/api/v1")
@RestController
public class UserController {
    @Autowired
    UserValidationService userValidationService;

    @GetMapping("/username")
    @ResponseBody
    public ResponseEntity<User> getUserName(@RequestHeader(value = "X-Auth-Username", required = false) String authUsername, @RequestHeader(value = "X-Auth-Email", required = false) String email) {
        return ResponseEntity.ok().body(userValidationService.getUserService(authUsername, email));
    }
}

