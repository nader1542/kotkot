package com.jackrutorial;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.RowMapper;

import com.jackrutorial.model.User;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

 @Autowired
 public JobBuilderFactory jobBuilderFactory;
 
 @Autowired
 public StepBuilderFactory stepBuilderFactory;
 
 @Autowired
 public DataSource dataSource;

 
 @Bean
 public JdbcCursorItemReader<User> reader(){
  JdbcCursorItemReader<User> reader = new JdbcCursorItemReader<User>();
  reader.setDataSource(dataSource);
  reader.setSql("SELECT id,name FROM user");
  reader.setRowMapper(new UserRowMapper());
  
  return reader;
 }
 
 public class UserRowMapper implements RowMapper<User>{

  @Override
  public User mapRow(ResultSet rs, int rowNum) throws SQLException {
   User user = new User();
   user.setId(rs.getInt("id"));
   user.setName(rs.getString("name"));
   
   return user;
  }
  
 }
 
 @Bean
 public UserItemProcessor processor(){
  return new UserItemProcessor();
 }
 
 @Bean
 public FlatFileItemWriter<User> writer(){
  FlatFileItemWriter<User> writer = new FlatFileItemWriter<User>();
  writer.setResource(new ClassPathResource("users.csv"));
  writer.setLineAggregator(new DelimitedLineAggregator<User>() {{
   setDelimiter(",");
   setFieldExtractor(new BeanWrapperFieldExtractor<User>() {{
    setNames(new String[] { "id", "name" });
   }});
  }});
  
  return writer;
 }
 
 
 
 @Bean
 public Step step1() {
  return stepBuilderFactory.get("step1").<User, User> chunk(3)
    .reader(reader())
    .processor(processor())
    .writer(writer())
    .build();
 }
 
 @Bean
 public Job exportUserJob() {
  return jobBuilderFactory.get("exportUserJob")
    .incrementer(new RunIdIncrementer())
    .flow(step1())
    .end()
    .build();
 }
 
}