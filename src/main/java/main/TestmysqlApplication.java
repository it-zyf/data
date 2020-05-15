package main;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@SpringBootApplication
@EnableScheduling
@ComponentScan(basePackages = {"runner","controller"})

//@EnableAsync
public class TestmysqlApplication {
    public static void main(String[] args) {
        SpringApplication.run(TestmysqlApplication.class, args);

    }
}