package com.infotech.batch.model;

import lombok.Data;

@Data
public class Person {

	private long id;
	private String firstName;
	private String lastName;
	private String gender;
	private int age;
	private String fullName;

	public String getFullName() {
		return fullName = this.firstName + " " + this.lastName;
	}

}