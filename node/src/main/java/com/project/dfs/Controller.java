package com.project.dfs;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@RestController
@RequestMapping()
public class Controller {

    @Value("${server.port}")
    private int portNumber;

    @GetMapping("/downloadFile")
    public ResponseEntity<byte[]> downloadFile(@RequestParam(name = "fileName") String fileName) throws IOException {
        String filePath = "/fs/" + portNumber + "/" + fileName + ".txt";
        byte[] bytes = Files.readAllBytes(Paths.get(filePath));
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("application/octet-stream"))
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + "/fs/" + portNumber + "/" + fileName + ".txt")
                .body(bytes);
    }

}

