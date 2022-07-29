/****************************************************************************/
/* Copyright 2015 University of Texas                                       */
/*                                                                          */
/* Licensed under the Apache License, Version 2.0 (the "License");          */
/* you may not use this file except in compliance with the License.         */
/* You may obtain a copy of the License at                                  */
/*                                                                          */
/*     http://www.apache.org/licenses/LICENSE-2.0                           */
/*                                                                          */
/* Unless required by applicable law or agreed to in writing, software      */
/* distributed under the License is distributed on an "AS IS" BASIS,        */
/* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. */
/* See the License for the specific language governing permissions and      */
/* limitations under the License.                                           */
/****************************************************************************/

package karnak.service.train

import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

import java.util.Date

import com.weiglewilczek.slf4s.Logging

import cml.learn._

/*********************************************************************************************************************/

object KarnakDatabase extends Logging{
  Class.forName("com.mysql.jdbc.Driver");

  private def getConnection(): Connection = {
    return java.sql.DriverManager.getConnection("jdbc:mysql://localhost/karnak","karnak","");
  }

  /*******************************************************************************************************************/

  def readUnsubmittedExperiences(system: String, earliestTime: Date): Vector[Experience] = {
    logger.info("creating unsubmitted schema for "+system)
    val schema = getUnsubmittedSchema(system)

    val command = "select * from unsubmitted_experiences where system='"+system+"'"+
                  " and startTime >= '"+karnak.service.WebService.dateToSqlString(earliestTime)+"'"
    readUnsubmittedExperiences(command,schema)
  }

  def readUnsubmittedExperiences(system: String, earliestTime: Date, latestTime: Date): Vector[Experience] = {
    logger.info("creating unsubmitted schema for "+system)
    val schema = getUnsubmittedSchema(system)

    val command = "select * from unsubmitted_experiences where system='"+system+"'"+
                  " and startTime >= '"+karnak.service.WebService.dateToSqlString(earliestTime)+"'"+
                  " and startTime < '"+karnak.service.WebService.dateToSqlString(latestTime)+"'"
                  /* Should not it be readUnsubmittedExperiences ? 
                      Jungha Woo 
                  */
    readSubmittedExperiences(command,schema) 
  }

  private def readUnsubmittedExperiences(selectCommand: String, schema: ExperienceSchema): Vector[Experience] = {
    logger.info("reading unsubmitted experiences...")
    var experiences = Vector[Experience]()
    val conn = getConnection()
    val stat = conn.createStatement()
    val rs = stat.executeQuery(selectCommand)
    while (rs.next()) {
      val exp = createUnsubmittedExperience(rs,schema)
      try {
        schema.validate(exp)
        experiences = experiences :+ exp
      } catch {
        case e: cml.learn.SchemaException => logger.warn("ignoring experience: %s".format(e))
      }
    }
    rs.close()
    stat.close()
    conn.close()

    experiences
  }

  /*
   Client doesn't provide user or project name when requesting an unsubmitted experience, so no features
   related to those.
   */
  def getUnsubmittedSchema(system: String): ExperienceSchema = {
    val schema = new ExperienceSchema(system+"-unsubmitted")

    //schema.features = schema.features :+ new CategoryFeatureSchema(Symbol("queue"),true,getQueueNames(system))
    schema.features = schema.features :+ new CategoryFeatureSchema(Symbol("queue"),true,Set())
    schema.features = schema.features :+ new IntFeatureSchema(Symbol("processors"),true)
    schema.features = schema.features :+ new IntFeatureSchema(Symbol("requestedWallTime"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("time"),true)

    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countAhead"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workAhead"),true)
    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countRunning"),true)
    schema.features = schema.features :+ new IntFeatureSchema(Symbol("processorsRunning"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workRunning"),true)

    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countAheadQueue"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workAheadQueue"),true)

    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countAheadOtherQueues"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workAheadOtherQueues"),true)

    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countAheadLessEqualProcs"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workAheadLessEqualProcs"),true)

    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countAheadLessEqualWork"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workAheadLessEqualWork"),true)

    schema.features = schema.features :+ new LongFeatureSchema(Symbol("waitTime"),false)

    schema
  }

  private def createUnsubmittedExperience(rs: ResultSet, schema: ExperienceSchema): Experience = {
    var features = Vector[Option[Any]]()
    val system = rs.getString("system")
    //features = features :+ rs.getString("system")
    features = features :+ getSymbol(rs,"queue")
    features = features :+ getInt(rs,"processors")
    features = features :+ getInt(rs,"requestedWallTime")
    val timeEpoch = rs.getTimestamp("time").getTime() / 1000      // seconds since epoch
    features = features :+ Some(timeEpoch)

    features = features :+ getInt(rs,"countAhead")
    features = features :+ getLong(rs,"workAhead")
    features = features :+ getInt(rs,"countRunning")
    features = features :+ getInt(rs,"processorsRunning")
    features = features :+ getLong(rs,"workRunning")

    features = features :+ getInt(rs,"countAheadQueue")
    features = features :+ getLong(rs,"workAheadQueue")

    features = features :+ getInt(rs,"countAheadOtherQueues")
    features = features :+ getLong(rs,"workAheadOtherQueues")

    features = features :+ getInt(rs,"countAheadLessEqualProcs")
    features = features :+ getLong(rs,"workAheadLessEqualProcs")

    features = features :+ getInt(rs,"countAheadLessEqualWork")
    features = features :+ getLong(rs,"workAheadLessEqualWork")

    //features = features :+ getBoolean(rs,"jobsRunning")

    val startEpoch = rs.getTimestamp("startTime").getTime() / 1000
    features = features :+ Some(startEpoch - timeEpoch)                      // wait time

    new Experience(features)
  }

  /*******************************************************************************************************************/

  def readSubmittedExperiences(system: String, earliestTime: Date): Vector[Experience] = {
    logger.info("creating submitted schema for "+system)
    val schema = getSubmittedSchema(system)

    val command = "select * from submitted_experiences where system='"+system+"'"+
                  " and startTime >= '"+karnak.service.WebService.dateToSqlString(earliestTime)+"'"
    readSubmittedExperiences(command,schema)
  }

  def readSubmittedExperiences(system: String, earliestTime: Date, latestTime: Date): Vector[Experience] = {
    logger.info("creating submitted schema for "+system)
    val schema = getSubmittedSchema(system)

    val command = "select * from submitted_experiences where system='"+system+"'"+
                  " and startTime >= '"+karnak.service.WebService.dateToSqlString(earliestTime)+"'"+
                  " and startTime < '"+karnak.service.WebService.dateToSqlString(latestTime)+"'"
    readSubmittedExperiences(command,schema)
  }

  /* 07/12/2016 
    These two read functions read non-simulated wait time experiences.
    Three experience objects have been created per a real observation.
    therefore we extract the real observation only out of 3 experience having the same system, job id, and start time.
  */

  def readSubmittedObservedExperiences(system: String, earliestTime: Date): Vector[Experience] = {
    logger.info("creating submitted schema for "+system)
    val schema = getSubmittedSchema(system)
    val command = "select * from submitted_experiences where system='"+system+"'"+
                  " and startTime >= '"+karnak.service.WebService.dateToSqlString(earliestTime)+"' group by id, startTime having time = min(time)"
    readSubmittedExperiences(command,schema)
  }

  def readSubmittedObservedExperiences(system: String, earliestTime: Date, latestTime: Date): Vector[Experience] = {
    logger.info("creating submitted schema for "+system)
    val schema = getSubmittedSchema(system)

    val command = "select * from submitted_experiences where system='"+system+"'"+
                  " and startTime >= '"+karnak.service.WebService.dateToSqlString(earliestTime)+"'"+
                  " and startTime < '"+karnak.service.WebService.dateToSqlString(latestTime)+"' group by id, startTime having time = min(time)"
    readSubmittedExperiences(command,schema)
  }





  private def readSubmittedExperiences(selectCommand: String, schema: ExperienceSchema): Vector[Experience] = {
    logger.info("reading submitted experiences...")
    var experiences = Vector[Experience]()
    val conn = getConnection()
    val stat = conn.createStatement()
    val rs = stat.executeQuery(selectCommand)
    while (rs.next()) {
      val exp = createSubmittedExperience(rs,schema)
      schema.validate(exp)
      experiences = experiences :+ exp
    }
    rs.close()
    stat.close()
    conn.close()

    experiences
  }

  private def createSubmittedExperience(rs: ResultSet, schema: ExperienceSchema): Experience = {
    var features = Vector[Option[Any]]()
    val system = rs.getString("system")
    //features = features :+ rs.getString("system")
    //features = features :+ rs.getString("id")
    //features = features :+ rs.getString("name")
    //features = features :+ rs.getString("user")
    features = features :+ getSymbol(rs,"queue")
    //features = features :+ rs.getString("project")
    features = features :+ getInt(rs,"processors")
    features = features :+ getInt(rs,"requestedWallTime")
    val timeEpoch = rs.getTimestamp("time").getTime() / 1000      // seconds since epoch
    features = features :+ Some(timeEpoch)

    features = features :+ getInt(rs,"countAhead")
    features = features :+ getLong(rs,"workAhead")
    features = features :+ getInt(rs,"countRunning")
    features = features :+ getInt(rs,"processorsRunning")
    features = features :+ getLong(rs,"workRunning")

    features = features :+ getInt(rs,"countAheadUser")
    features = features :+ getLong(rs,"workAheadUser")
    features = features :+ getInt(rs,"countRunningUser")
    features = features :+ getInt(rs,"processorsRunningUser")
    features = features :+ getLong(rs,"workRunningUser")

    features = features :+ getInt(rs,"countAheadProject")
    features = features :+ getLong(rs,"workAheadProject")
    features = features :+ getInt(rs,"countRunningProject")
    features = features :+ getInt(rs,"processorsRunningProject")
    features = features :+ getLong(rs,"workRunningProject")

    features = features :+ getInt(rs,"countAheadQueue")
    features = features :+ getLong(rs,"workAheadQueue")

    features = features :+ getInt(rs,"countAheadOtherQueues")
    features = features :+ getLong(rs,"workAheadOtherQueues")

    features = features :+ getInt(rs,"countAheadLessEqualProcs")
    features = features :+ getLong(rs,"workAheadLessEqualProcs")

    features = features :+ getInt(rs,"countAheadLessEqualWork")
    features = features :+ getLong(rs,"workAheadLessEqualWork")

    //features = features :+ getBoolean(rs,"jobsRunning")

    val startEpoch = rs.getTimestamp("startTime").getTime() / 1000
    features = features :+ Some(startEpoch - timeEpoch)                      // wait time

    new Experience(features)
  }

  def getSubmittedSchema(system: String): ExperienceSchema = {
    val schema = new ExperienceSchema(system+"-submitted")

    //schema.features = schema.features :+ new CategoryFeatureSchema(Symbol("queue"),true,getQueueNames(system))
    schema.features = schema.features :+ new CategoryFeatureSchema(Symbol("queue"),true,Set())
    schema.features = schema.features :+ new IntFeatureSchema(Symbol("processors"),true)
    schema.features = schema.features :+ new IntFeatureSchema(Symbol("requestedWallTime"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("time"),true)

    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countAhead"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workAhead"),true)
    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countRunning"),true)
    schema.features = schema.features :+ new IntFeatureSchema(Symbol("processorsRunning"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workRunning"),true)

    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countAheadUser"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workAheadUser"),true)
    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countRunningUser"),true)
    schema.features = schema.features :+ new IntFeatureSchema(Symbol("processorsRunningUser"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workRunningUser"),true)

    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countAheadProject"),false)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workAheadProject"),false)
    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countRunningProject"),false)
    schema.features = schema.features :+ new IntFeatureSchema(Symbol("processorsRunningProject"),false)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workRunningProject"),false)

    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countAheadQueue"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workAheadQueue"),true)

    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countAheadOtherQueues"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workAheadOtherQueues"),true)

    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countAheadLessEqualProcs"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workAheadEqualProcs"),true)

    schema.features = schema.features :+ new IntFeatureSchema(Symbol("countAheadLessEqualWork"),true)
    schema.features = schema.features :+ new LongFeatureSchema(Symbol("workAheadLessEqualWork"),true)

    schema.features = schema.features :+ new LongFeatureSchema(Symbol("waitTime"),false)

    schema
  }

  /*******************************************************************************************************************/

  private def getString(rs: ResultSet, colName: String): Option[String] = {
    rs.getString(colName) match {
      case null => None
      case str => Some(str)
    }
  }

  private def getSymbol(rs: ResultSet, colName: String): Option[Symbol] = {
    rs.getString(colName) match {
      case null => None
      case sym => Some(Symbol(sym))
    }
  }

  private def getBoolean(rs: ResultSet, colName: String): Option[Boolean] = {
    val b = rs.getBoolean(colName)
    rs.wasNull() match {
      case true => None
      case false => Some(b)
    }
  }

  private def getInt(rs: ResultSet, colName: String): Option[Int] = {
    val num = rs.getInt(colName)
    rs.wasNull() match {
      case true => None
      case false => Some(num)
    }
  }

  private def getLong(rs: ResultSet, colName: String): Option[Long] = {
    val num = rs.getLong(colName)
    rs.wasNull() match {
      case true => None
      case false => Some(num)
    }
  }

  private val historyMilliSecs = 8 * 7 * 24 * 60 * 60 * 1000  // 8 weeks

  def getSystemNames(): Set[Symbol] = {
    var names = Set[Symbol]()

    val conn = getConnection()
    val stat = conn.createStatement()
    val now = new Date()
    val earliestTime = new Date(now.getTime() - historyMilliSecs)
    val command = "select distinct(system) from submitted_experiences"
    //val command = "select distinct(system) from submitted_experiences where time >= '"+
    //              karnak.service.Service.dateToSqlString(earliestTime)+"'"
    val rs = stat.executeQuery(command)
    while (rs.next()) {
      names = names + Symbol(rs.getString("system"))
    }
    rs.close()
    stat.close()
    conn.close()

    names
  }

  def getQueueNames(system: String): Set[Symbol] = {
    updateQueueNames(system)
    queueNames(system)
  }

  private var queueNamesUpdate = Map[String,Date]()
  private var queueNames = Map[String,Set[Symbol]]()

  private def updateQueueNames(system: String): Unit = {
    val now = new Date()
    if (!queueNamesUpdate.contains(system) || (now.getTime() - queueNamesUpdate(system).getTime() > 60 * 60 * 1000)) {
      queueNamesUpdate = queueNamesUpdate + (system -> now)
      queueNames = queueNames + (system -> readQueueNames(system))
    }
  }

  def readQueueNames(system: String): Set[Symbol] = {
    var names = Set[Symbol]()

    val now = new Date()
    val earliestTime = new Date(now.getTime() - historyMilliSecs)

    val conn = getConnection()
    val stat = conn.createStatement()
    val command = "select distinct(queue) from unsubmitted_experiences where system='"+system+"'"
    val rs = stat.executeQuery(command)
    while (rs.next()) {
      names = names + Symbol(rs.getString("queue"))
    }
    rs.close()
    val rs2 = stat.executeQuery("select distinct(queue) from submitted_experiences where system='"+system+"'")
    while (rs2.next()) {
      names = names + Symbol(rs2.getString("queue"))
    }
    rs2.close()
    stat.close()
    conn.close()

    names
  }

}

/*********************************************************************************************************************/
