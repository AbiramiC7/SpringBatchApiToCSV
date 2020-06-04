package com.infotech.batch.config;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.infotech.batch.model.Person;

public class PersonReader implements ItemReader<Person> {

		private static final Logger LOGGER = LoggerFactory.getLogger(PersonReader.class);

		private final String apiUrl;
		private final RestTemplate restTemplate;

		private int nextPersonIndex;
		private List<Person> personsList;

		public PersonReader(String apiUrl, RestTemplate restTemplate) {
			this.apiUrl = apiUrl + PersonBatchConstants.GET_PERSONS;
			this.restTemplate = restTemplate;
		}

		@Override
		public Person read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
			if (null == this.personsList || this.personsList.isEmpty()) {
				this.personsList = retrievePersonFromApi();
			}
			Person nextPerson = null;

			if (nextPersonIndex < personsList.size()) {
				nextPerson = personsList.get(nextPersonIndex);
				LOGGER.info("Reading Person detail : {}",nextPersonIndex);
				nextPersonIndex++;
			}
			return nextPerson;
		}

		private List<Person> retrievePersonFromApi() {
			HttpHeaders headers = new HttpHeaders();
			HttpEntity<String> request = new HttpEntity<String>(headers);
			
			ResponseEntity<Person[]> response  = restTemplate.exchange
			 (apiUrl, HttpMethod.GET, request, Person[].class);
			
			Person[] personDto = response.getBody();
			return Arrays.asList(personDto);
		}

	}

