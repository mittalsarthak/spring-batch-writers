package com.swt.batchwriters;

import com.swt.batchwriters.model.Product;
import com.swt.batchwriters.processor.ProductProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

@EnableBatchProcessing
@Configuration
public class BatchConfig {

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private DataSource datasource;

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

        writer.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("productId|productName|productDesc|price|unit");
            }
        });

        //To make the writer append in existing file
        //writer.setAppendAllowed(true);

        writer.setFooterCallback(new FlatFileFooterCallback() {
            @Override
            public void writeFooter(Writer writer) throws IOException {
                writer.write("The file is created on " + new SimpleDateFormat().format(new Date()));
            }
        });
        return writer;
    }

    @Bean
    @StepScope
    public StaxEventItemWriter xmlWriter(
            @Value("#{jobParameters['outputFile']}")
            FileSystemResource outputFile
    ){
        XStreamMarshaller marshaller = new XStreamMarshaller();

        //Add aliases for the tag
        HashMap<String, Class> alias = new HashMap<>();
        alias.put("Product",Product.class);
        marshaller.setAliases(alias);
        marshaller.setAutodetectAnnotations(true);

        StaxEventItemWriter writer = new StaxEventItemWriter();
        writer.setResource(outputFile);
        writer.setMarshaller(marshaller);
        writer.setRootTagName("Products");

        return writer;
    }

    @Bean
    public JdbcBatchItemWriter jdbcBatchItemWriter(){
        JdbcBatchItemWriter writer = new JdbcBatchItemWriter();
        writer.setDataSource(datasource);
        writer.setSql("INSERT INTO products (\"productId\",\"productName\",\"productDesc\",price,unit) " +
                "VALUES (?, ?, ?, ?, ?);");
        writer.setItemPreparedStatementSetter(new ItemPreparedStatementSetter<Product>() {
            @Override
            public void setValues(Product item, PreparedStatement ps) throws SQLException {
                ps.setInt(1,item.getProductId());
                ps.setString(2, item.getProductName());
                ps.setString(3, item.getProductDesc());
                ps.setBigDecimal(4,item.getPrice());
                ps.setInt(5,item.getUnit());
            }
        });
        return writer;
    }
    //Another way to create a writer in database
    @Bean
    public JdbcBatchItemWriter dbwriter(){
        return new JdbcBatchItemWriterBuilder<Product>()
                .dataSource(this.datasource)
                .sql("INSERT INTO products (\"productId\",\"productName\",\"productDesc\",price,unit) " +
                        "VALUES (:productId,:productName,:productDesc,:price,:unit);")
                .beanMapped()
                .build();
    }

    @Bean
    public Step step1(){
        return steps.get("step1").
                <Product,Product>chunk(1)
                .reader(flatFileItemReader(null))
//                .writer(flatFileItemWriter(null))
                .writer(xmlWriter(null))
//                .writer(jdbcBatchItemWriter())
//                .writer(dbwriter())
                .processor(new ProductProcessor())
                .build();
    }

    @Bean
    public Job batchWriteJob(){
        return jobs.get("batchWriteJob")
                .incrementer(new RunIdIncrementer())
                .start(step1())
                .build();
    }


}
