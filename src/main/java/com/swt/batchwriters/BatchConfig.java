package com.swt.batchwriters;

import com.swt.batchwriters.model.Product;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@EnableBatchProcessing
@Configuration
public class BatchConfig {

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private JobBuilderFactory jobs;

    @StepScope
    @Bean
    public FlatFileItemReader flatFileItemReader(
            @Value("#{jobParameters['inputFile']}")
            FileSystemResource inputFile
    ){
        FlatFileItemReader reader = new FlatFileItemReader();
        reader.setResource(inputFile);
        reader.setLinesToSkip(1);
        reader.setLineMapper(new DefaultLineMapper(){
            {
                setFieldSetMapper(new BeanWrapperFieldSetMapper(){
                    {
                        setTargetType(Product.class);
                    }
                });

                setLineTokenizer(new DelimitedLineTokenizer(){
                    {
                        setNames(new String[] {"productId","productName","productDesc","price","unit"});
                        setDelimiter(",");
                    }
                });
            }
        });
        return reader;
    }

    @StepScope
    @Bean
    public FlatFileItemWriter flatFileItemWriter(
            @Value("#{JobParameters['outputFile']}")
            FileSystemResource outputFile
    ){
        FlatFileItemWriter writer = new FlatFileItemWriter();
        writer.setResource(outputFile);

        writer.setLineAggregator(new DelimitedLineAggregator(){
            {
                setDelimiter("|");
                setFieldExtractor(new BeanWrapperFieldExtractor(){
                    {
                        setNames(new String[]{"productId","productName","productDesc","price","unit"});
                    }
                });
            }
        });
        return writer;
    }

    @Bean
    public Step step1(){
        return steps.get("step1").
                <Product,Product>chunk(1)
                .reader(flatFileItemReader(null))
                .writer(flatFileItemWriter(null))
                .build();
    }

    @Bean
    public Job batchWriteJob(){
        return jobs.get("batchWriteJob")
                .start(step1())
                .build();
    }


}
