package de.herrlau.betterreadsdataloader;

import de.herrlau.betterreadsdataloader.author.Author;
import de.herrlau.betterreadsdataloader.author.AuthorRepository;
import de.herrlau.betterreadsdataloader.book.Book;
import de.herrlau.betterreadsdataloader.book.BookRepository;
import de.herrlau.betterreadsdataloader.connection.DataStaxAstraProperties;
import org.json.JSONArray;
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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterReadsDataLoaderApplication {
    @Autowired
    AuthorRepository authorRepository;
    @Autowired
    BookRepository bookRepository;

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
        //loadAuthors();
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
                            // construct book
                            Book book = new Book();
                            book.setId(jsonObject.getString("key").replace("/works/", ""));
                            book.setName(jsonObject.optString("title"));

                            JSONObject description = jsonObject.optJSONObject("description");
                            if (description != null)
                                book.setDescription(description.optString("value"));

                            JSONObject published = jsonObject.getJSONObject("created");
                            if (published != null) {
                                var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
                                LocalDate date = LocalDate.parse(published.getString("value"),formatter);
                                book.setPublishedDate(date);
                            }

                            JSONArray covers = jsonObject.optJSONArray("covers");
                            List<String> coverIds = new ArrayList<>();
                            for (int i = 0; covers != null && i < covers.length(); i++) {
                                coverIds.add(covers.getString(i));
                            }
                            book.setCoverIds(coverIds);

                            JSONArray authors = jsonObject.getJSONArray("authors");
                            List<String> authorIds = new ArrayList<>();
                            for (int i = 0; authors != null && i < authors.length(); i++) {
                                String authorId = authors.getJSONObject(i)
                                        .getJSONObject("author")
                                        .getString("key")
                                        .replace("/authors/", "");
                                authorIds.add(authorId);
                            }
                            book.setAuthorIds(authorIds);

                            var authorsList = authorIds.stream()
                                    .map(id -> authorRepository.findById(id))
                                    .map(author -> {
                                                if (author.isPresent()) return author.get().getName();
                                                else return "n/a";
                                            }
                                    )
                                    .collect(Collectors.toList());
                            book.setAuthorNames(authorsList);

                            bookRepository.save(book);
                            System.out.println("Loaded; " + book);
                        }
                        catch (JSONException e) {
                            System.err.println(e.getMessage());
                        }
                    }
            );
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
