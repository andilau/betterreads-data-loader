package de.herrlau.betterreadsdataloader;

import de.herrlau.betterreadsdataloader.author.Author;
import de.herrlau.betterreadsdataloader.author.AuthorRepository;
import de.herrlau.betterreadsdataloader.connection.DataStaxAstraProperties;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterReadsDataLoaderApplication {
    @Autowired
    AuthorRepository authorRepository;

    @Value("classpath:ol-dump-authors.txt")
    Resource authorsFile;
    @Value("classpath:ol-dump-works.txt")
    Resource worksFile;

    public static void main(String[] args) {
        SpringApplication.run(BetterReadsDataLoaderApplication.class, args);
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

    @PostConstruct
    public void start() {
        loadAuthors();
        loadWorks();
    }

    public void loadAuthors() {
        try (Stream<String> lines = Files.lines(authorsFile.getFile().toPath())) {
            lines.forEach(line -> {
                        // parse data
                        String json = line.substring(line.indexOf("{"));
                        try {
                            JSONObject jsonObject = new JSONObject(json);
                            // construct author
                            Author author = new Author();
                            author.setId(jsonObject.optString("key").replace("/authors/", ""));
                            author.setName(jsonObject.optString("name"));
                            author.setPersonalName(jsonObject.optString("personal_name"));
                            // save to repository
                            authorRepository.save(author);
                            System.out.println("Author saved: " + author.getName());
                        }
                        catch (JSONException e) {
                            e.printStackTrace();
                        }
                    }
            );
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadWorks() {
        try (Stream<String> lines = Files.lines(worksFile.getFile().toPath())) {
            lines.forEach(line -> {
                        // parse data
                        String json = line.substring(line.indexOf("{"));
                        try {
                            JSONObject jsonObject = new JSONObject(json);
                            // construct author
                        }
                        catch (JSONException e) {
                            e.printStackTrace();
                        }
                    }
            );
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
