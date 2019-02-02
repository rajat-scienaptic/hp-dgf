package com.scienaptic.jobs.config

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.scienaptic.java.helper.Generics


class ConfigurationFactory[A <: Configuration](args: Array[String]) {

//  private val cliOptions = new Options
//
//  cliOptions.addOption("c", "configuration", true, "path to configuration file")

//  private val cliParser = new DefaultParser

  private val objectMapper = new ObjectMapper()

  objectMapper.registerModule(DefaultScalaModule)

  private val configFileLocation: String = {
    args(0) match {
      case "-c"  => args(1)
      case _ => {
        print("Invalid Option / Please pass the config file\r\n For eg 'project.jar -c configuration.yml'")
        System.exit(1)
        ""
      }
    }
    /*cliParser.parse(cliOptions, args) match {
      case c if {
        c.hasOption("c")
      } => c.getOptionValue("c")
      case _ => {
        print("Invalid Option / Please pass the config file\r\n For eg 'project.jar -c configuration.yml'")
        System.exit(1)
        ""
      }
    }*/
  }

  protected val configuration = objectMapper.readValue(new File(configFileLocation), Generics.getTypeParameter(getClass, classOf[Configuration])).asInstanceOf[A]
}
