package com.infotech.batch.config;


import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.FileSystemResource;
import org.springframework.web.client.RestTemplate;

import com.infotech.batch.model.Person;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
	@Bean
	ItemReader<Person> restPersonReader(Environment environment, RestTemplate restTemplate) {
		return new PersonReader(environment.getRequiredProperty(PersonBatchConstants.API_URL), restTemplate);
	}
		
	@Bean
	ItemWriter<Person> restPersonWriter() {
		FlatFileItemWriter<Person> writer = new FlatFileItemWriter<Person>();
		String exportFilePath = PersonBatchConstants.CSV_FILE_PATH;
		writer.setResource(new FileSystemResource(exportFilePath));
		writer.setAppendAllowed(true);
		DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<Person>();
		lineAggregator.setDelimiter(",");
		
		BeanWrapperFieldExtractor<Person>  fieldExtractor = new BeanWrapperFieldExtractor<Person>();
		fieldExtractor.setNames(new String[]{"id","firstName","lastName","gender","age","fullName"});
		lineAggregator.setFieldExtractor(fieldExtractor);
		
		writer.setLineAggregator(lineAggregator);
		return writer;
		
	}

	@Bean
	Step apiServiceToCSVFileStep(ItemReader<Person> restPersonReader,
			ItemWriter<Person> restPersonWriter,
			StepBuilderFactory stepBuilderFactory) {
		return stepBuilderFactory.get("apiServiceToCSVFileStep").<Person, Person>chunk(1).reader(restPersonReader)
				.writer(restPersonWriter).build();
	}

	@Bean
	Job apiServiceToCSVFileJob(@Qualifier("apiServiceToCSVFileStep") Step apiToCSVPersonStep,
			JobBuilderFactory jobBuilderFactory) {
		return jobBuilderFactory.get("apiServiceToCSVFileJob").incrementer(new RunIdIncrementer())
				.flow(apiToCSVPersonStep).end().build();
	}
}