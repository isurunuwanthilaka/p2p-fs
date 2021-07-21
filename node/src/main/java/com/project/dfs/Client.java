package com.project.dfs;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

@Service
public class Client {

    @Value("${server.port}")
    private int ownerPortNumber;

    RestTemplate restTemplate=new RestTemplate();

    public void getFile(int portNumber,String fileName) {
        try {
            Node.log(Node.INFO,portNumber+fileName);
            HttpHeaders headers = new HttpHeaders();
            URI url = UriComponentsBuilder.fromUriString("http://localhost:"+portNumber+"/downloadFile").queryParam("fileName",fileName).build().toUri();
            headers.setAccept(Arrays.asList(MediaType.APPLICATION_OCTET_STREAM));
            HttpEntity<String> entity = new HttpEntity<>(headers);
            Node.log(Node.INFO,"Url "+url.toString());
            ResponseEntity<byte[]> response = restTemplate.exchange(url, HttpMethod.GET, entity, byte[].class);
            Files.write(Paths.get("D:/MSc/DC-Group project/p2p/fs/" + "7770" + "/" + fileName + ".txt"), response.getBody());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}