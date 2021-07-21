package com.isuru.dfs;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/file")
public class Controller {

    @GetMapping("/hello")
    public String hello() {
        return "Hello";
    }
}
