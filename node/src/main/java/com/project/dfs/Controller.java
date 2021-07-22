package com.project.dfs;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@RestController
@RequestMapping()
public class Controller {

    @Value("${server.port}")
    private int portNumber;

    @GetMapping("/downloadFile")
    public ResponseEntity<byte[]> downloadFile(@RequestParam(name = "fileName") String fileName) throws IOException, NoSuchAlgorithmException {
        Utils.nowEpoch("DOWNLOAD REQ RECEIVED");
        String filePath = "/fs/" + portNumber + "/" + fileName + ".txt";

        //create random content to fie
        Utils.createRandomContentToFile(filePath);

        //file size
        long size = Files.size(Paths.get(filePath));
        Node.log("INFO", "Requested file size : " + size / 1048576 + " MB");

        //file SHA
        MessageDigest shaDigest = MessageDigest.getInstance("SHA-256");
        File file = new File(filePath);
        String shaChecksum = Utils.getFileChecksum(shaDigest, file);
        Node.log("INFO", "Requested file SHA : " + shaChecksum);

        byte[] bytes = Files.readAllBytes(Paths.get(filePath));
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("application/octet-stream"))
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + "/fs/" + portNumber + "/" + fileName + ".txt")
                .body(bytes);
    }

}

