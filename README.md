# HP DGF TOOL (Back-End)

This project is a part of HP Ether 2.0. 

### Prerequisites
* Git
* JDK 8 or later
* Maven 3.0 or later

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management
* [JDK](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) - Java™ Platform, Standard Edition Development Kit 
* [Spring Boot](https://spring.io/projects/spring-boot) - Framework to ease the bootstrapping and development of new Spring Applications
* [git](https://git-scm.com/) - Free and Open-Source distributed version control system 
* [Postman](https://www.getpostman.com/) - API Development Environment (Testing Docmentation)
* [Open API Swagger] - Open-Source software framework backed by a large ecosystem of tools that helps developers design, build, document, and consume RESTful Web services.

## Points covered as per the given tasks

- API Endpoints created for fetching and saving data to/from database.
- MariaDB has been used for database layer.
- Code published on Github and deployed on Ether 2.0.
- Open API Swagger integration added
- Implemented Custom and Global Exception handling. 
- Keycloak integration.
- README.md file added.

## Running the application locally

There are several ways to run a Spring Boot application on your local machine. One way is to execute the `main` method in the `com.hp.dgf.Application` class from your IDE.

- Download the zip or clone the Git repository.
- Unzip the zip file (if you downloaded one)
- Open Command Prompt and Change directory (cd) to folder containing pom.xml
- Open Eclipse 
   - File -> Import -> Existing Maven Project -> Navigate to the folder where you unzipped the zip
   - Select the project
- Choose the Spring Boot Application file (search for @SpringBootApplication)
- Right Click on the file and Run as Java Application

Alternatively you can use the [Spring Boot Maven plugin](https://docs.spring.io/spring-boot/docs/current/reference/html/build-tool-plugins-maven-plugin.html) like so:

```shell
mvn spring-boot:run
```

## Files and Directories

The project (a.k.a. project directory) has a particular directory structure. A representative project is shown below:

```
.
├── Spring Elements
├── src
│   └── main
│       └── java
│           ├── com.hp.dgf
│           ├── com.hp.dgf.config
│           ├── com.hp.dgf.constants
│           ├── com.hp.dgf.controller
│           ├── com.hp.dgf.dto
│           ├── com.hp.dgf.exception
│           ├── com.hp.dgf.helper
│           ├── com.hp.dgf.model
│           └── com.hp.dgf.repository
│           └── com.hp.dgf.service
│           └── com.hp.dgf.service.impl
│           ├── com.hp.dgf.utils
├── src
│   └── main
│       └── resources
│           ├── application.yml
│           ├── build
├── src
│   └── test
│       └── java
│           └── com.hp.dgf
│           └── com.hp.dgf.controller
│           └── com.hp.dgf.service.impl
│ 
├── JRE System Library
├── Maven Dependencies
├── src
├── pom.xml
└── README.md
```

## Packages

- `config` -  All client configuration classes;
- `constants` - to define application constants;
- `controller` -  to listen to the client;
- `dto` -  to hold data transfer objects;
- `exception` -  to define custom and global exception classes;
- `helper` -  to hold helper class for generating excel report;
- `model` -  to hold model classes or POJO;
- `repository` -  to hold database layer logic;
- `service`  -  to define our business logic;
- `service/impl` -  to implement our business logic;
- `resources/` - Contains all the static resources, templates and property files.
- `resources/application.yml` - It contains application-wide properties. Spring reads the properties defined in this file to configure our application. We can define server’s default port, server’s context path, database URLs etc, in this file.

- `test/` - contains unit tests cases

- `pom.xml` - contains all the project dependencies


## Usage

* Get Header Data API
  -   Endpoint => localhost:9002/api/v1/getHeaderData/{businessCategoryId}
  -   Request Type => GET

* Add a new product line
  -   Endpoint => localhost:9002/api/v1/addPL
  -   Request Type => POST
  -   Example Input 
      - {
        	"code": "HF8",
        	"businessSubCategoryId": 1,
        	"baseRate": 1.00
        }

* Update an existing product line
  -   Endpoint => localhost:9002/api/v1/updatePL/{productLineId}
  -   Request Type => PUT
  
* Delete a product line
  -   Endpoint => localhost:9002/api/v1/deletePL/{productLineId}
  -   Request Type => DELETE
  
                
* [Swagger](http://localhost:9002/swagger-ui.html) - Swagger Open API Documentation


## Additional notes

- Used Lombok library for the generation of Getter, Setters and Default Constructors in Model Classes.
