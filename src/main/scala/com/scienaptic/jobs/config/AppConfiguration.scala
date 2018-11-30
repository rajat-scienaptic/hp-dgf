package com.scienaptic.jobs.config


import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.scienaptic.jobs.bean._

@JsonInclude(JsonInclude.Include.NON_NULL)
case class AppConfiguration(@JsonProperty("sparkConfig") val sparkConfig: SparkConfig,
                            @JsonProperty("sources") sources: Map[String, Source]) extends Configuration

case class SparkConfig(@JsonProperty("master") master: String,
                       @JsonProperty("appName") appName: String)

case class Source(@JsonProperty("name") name: String,
                  @JsonProperty("filePath") filePath: String,
                  @JsonProperty("select") selectOperation: Map[String, SelectOperation],
                  @JsonProperty("join") joinOperation: Map[String, JoinAndSelectOperation],
                  @JsonProperty("filter") filterOperation: Map[String, FilterOperation],
                  @JsonProperty("sort") sortOperation: Map[String, SortOperation],
                  @JsonProperty("union") unionOperation: Map[String, UnionOperation])

case class Join(@JsonProperty("leftTableAlias") leftTableAlias: String,
                @JsonProperty("rightTableAlias") rightTableAlias: String,
                @JsonProperty("typeOfJoin") typeOfJoin: String,
                @JsonProperty("joinCriteria") joinCriteria: Map[String, List[String]],
                @JsonProperty("selectCriteria") selectCriteria: Map[String, List[String]])
