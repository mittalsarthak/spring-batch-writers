package com.swt.batchwriters;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class BatchwritersApplication {
    private static ApplicationContext applicationContext;

    public static void main(String[] args) {
        SpringApplication.run(BatchwritersApplication.class, args);
    }

    public static void displayAllBeans() {
        String[] allBeanNames = applicationContext.getBeanDefinitionNames();
        for(String beanName : allBeanNames) {
            System.out.println(beanName);
        }
    }

}
