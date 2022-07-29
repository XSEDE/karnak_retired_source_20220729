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

package karnak.service.predict

import scala.collection.JavaConverters._
import scala.util.{Try,Success,Failure}

import com.weiglewilczek.slf4s._

import java.io.File
import java.nio.file.{Paths,Files}
import java.nio.file.attribute.FileTime
import java.util.Date

import cml.learn._

import karnak.KarnakException
import karnak.service._
import karnak.service.predict._
import karnak.service.train.{IO,KarnakDatabase}

/*********************************************************************************************************************/


trait PredictorUtils {

  protected def cmlValues(queryJob: Job, queueState: QueueState): Map[String,Option[Any]] = {
    var values = Map[String,Option[Any]]()

    values = values + ("queue" -> Some(Symbol(queryJob.queue)))
    values = values + ("processors" -> Some(queryJob.processors))
    values = values + ("requestedWallTime" -> Some(queryJob.requestedWallTime))

    values = values + ("time" -> Some(queueState.time.getTime() / 1000))

    val goodJobIds = queueState.jobIds.asScala.filter(jobId => queueState.jobs.containsKey(jobId))
    val orderedJobs = goodJobIds.map(jobId => queueState.jobs.get(jobId))
    val waitingJobs = orderedJobs.filter(job => job.pendingAtTime(queueState.time))
    val queryJobIndex = waitingJobs.zipWithIndex.filter(jobIndex => jobIndex._1.id.equals(queryJob.id))(0)._2

    val waitingJobsAhead = waitingJobs.take(queryJobIndex)
    values = values + ("countAhead" -> Some(waitingJobsAhead.size))
    values = values + ("workAhead" -> Some(waitingJobsAhead.map(job => job.work).sum))

    val runningJobs = orderedJobs.filter(job => (job.runningAtTime(queueState.time)))
    values = values + ("countRunning" -> Some(runningJobs.size))
    values = values + ("processorsRunning" -> Some(runningJobs.map(job => job.processors).sum))
    values = values + ("workRunning" -> Some(runningJobs.map(job => job.remainingWork(queueState.time)).sum))

    if (queryJob.user == null) {
      values = values + ("countAheadUser" -> None)
      values = values + ("workAheadUser" -> None)
      values = values + ("countRunningUser" -> None)
      values = values + ("processorsRunningUser" -> None)
      values = values + ("workRunningUser" -> None)
    } else {
      val userWaitingJobsAhead = waitingJobsAhead.filter(job => job.user.equals(queryJob.user))
      values = values + ("countAheadUser" -> Some(userWaitingJobsAhead.size))
      values = values + ("workAheadUser" -> Some(userWaitingJobsAhead.map(job => job.work).sum))
      val userRunningJobs = runningJobs.filter(job => job.user.equals(queryJob.user))
      values = values + ("countRunningUser" -> Some(userRunningJobs.size))
      values = values + ("processorsRunningUser" -> Some(userRunningJobs.map(job => job.processors).sum))
      values = values + ("workRunningUser" -> Some(userRunningJobs.map(job => job.remainingWork(queueState.time)).sum))
    }

    if (queryJob.project == null) {
      values = values + ("countAheadProject" -> None)
      values = values + ("workAheadProject" -> None)
      values = values + ("countRunningProject" -> None)
      values = values + ("processorsRunningProject" -> None)
      values = values + ("workRunningProject" -> None)
    } else {
      val projectWaitingJobsAhead = waitingJobsAhead.filter(job => job.user.equals(queryJob.user))
      values = values + ("countAheadProject" -> Some(projectWaitingJobsAhead.size))
      values = values + ("workAheadProject" -> Some(projectWaitingJobsAhead.map(job => job.work).sum))
      val projectRunningJobs = runningJobs.filter(job => job.user.equals(queryJob.user))
      values = values + ("countRunningProject" -> Some(projectRunningJobs.size))
      values = values + ("processorsRunningProject" -> Some(projectRunningJobs.map(job => job.processors).sum))
      values = values + ("workRunningProject" ->
        Some(projectRunningJobs.map(job => job.remainingWork(queueState.time)).sum))
    }

    val queueWaitingJobsAhead = waitingJobsAhead.filter(job => job.queue.equals(queryJob.queue))
    values = values + ("countAheadQueue" -> Some(queueWaitingJobsAhead.size))
    values = values + ("workAheadQueue" -> Some(queueWaitingJobsAhead.map(job => job.work).sum))

    val otherQueuesWaitingJobsAhead = waitingJobsAhead.filter(job => !job.queue.equals(queryJob.queue))
    values = values + ("countAheadOtherQueues" -> Some(otherQueuesWaitingJobsAhead.size))
    values = values + ("workAheadOtherQueues" -> Some(otherQueuesWaitingJobsAhead.map(job => job.work).sum))

    val lessEqualProcsWaitingJobsAhead = waitingJobsAhead.filter(job => job.processors <= queryJob.processors)
    values = values + ("countAheadLessEqualProcs" -> Some(lessEqualProcsWaitingJobsAhead.size))
    values = values + ("workAheadLessEqualProcs" -> Some(lessEqualProcsWaitingJobsAhead.map(job => job.work).sum))

    val lessEqualWorkWaitingJobsAhead = waitingJobsAhead.filter(job => job.work <= queryJob.work)
    values = values + ("countAheadLessEqualWork" -> Some(lessEqualWorkWaitingJobsAhead.size))
    values = values + ("workAheadLessEqualWork" -> Some(lessEqualWorkWaitingJobsAhead.map(job => job.work).sum))

    values = values + ("waitTime" -> None)

    values
  }

  protected def dropTime(schema: ExperienceSchema): ExperienceSchema = {
    val timeIndex = 3
    val (head,tail) = schema.features.splitAt(timeIndex)
    val s = new ExperienceSchema(schema.name)
    s.features = head ++ tail.tail
    s
  }

  protected def dropTime(query: Query): Query = {
    val timeIndex = 3
    val (head,tail) = query.features.splitAt(timeIndex)
    new Query(head ++ tail.tail,query.levelOfConfidence)
  }
}


/*********************************************************************************************************************/

object UnsubmittedPredictor extends PredictorUtils with Logging {
  def apply(name: String): UnsubmittedPredictor = {
    
    val path = "/home/karnak/karnak/etc/predictors/v1/%s/".format(name)
    logger.info("creating unsubmitted predictor for "+name+" at path:"+path+" weights:"+Vector(1,1,1,1,1))
    new UnsubmittedPredictor(name,dropTime(KarnakDatabase.getUnsubmittedSchema(name)), path, Vector(1,1,1,1,1))
  }

  def apply(name: String, path:String, assignedWeights: Vector[Float] ): UnsubmittedPredictor = {
    logger.info("creating unsubmitted predictor for "+name+" at path:"+path+" weights:"+assignedWeights)
    new UnsubmittedPredictor(name,dropTime(KarnakDatabase.getUnsubmittedSchema(name)), path, assignedWeights)
  }


  @throws(classOf[KarnakException])
  def getPrediction(query: UnsubmittedStartTimeQuery, weightOption: WeightingEnum ): UnsubmittedStartTimePrediction = {
    logger.info("predict unsubmitted")


    val predictor = weightOption match {
      case WeightingEnum.Bagging => getPredictor(query.system, "/home/karnak/karnak/etc/predictors/v1/%s/".format(query.system), Vector(1,1,1,1,1))
      case WeightingEnum.Single  => getPredictor(query.system, "/home/karnak/karnak/etc/predictors/v2/%s/".format(query.system), Vector(0,0,1,0,0)) 
    }
   
  /* private val pathPrefix = "/home/karnak/karnak/etc/predictors/%s/".format(name) */
  /* val predictor = getPredictor(query.system) */
    val q = toCmlQuery(query)
    val p = predictor.predict(dropTime(q)) match {
      case Success(p) => p
      case Failure(e: PredictException) => {
        throw new KarnakException(e.getMessage())
      }
      case Failure(e) => {
        throw new KarnakException("predict failed: %s".format(e))
      }
    }

    val pred = new UnsubmittedStartTimePrediction(query)

    val time = q.features(3) match {
      case Some(t: Long) => t
      case None => throw new KarnakException("didn't find a time to wait from")
      case Some(v) => throw new KarnakException("value is of unexpected type: %s".format(v))
    }
    pred.startTime = p.features(16) match {
      case Some(waitTime: Long) => new Date((time + waitTime) * 1000)
      case None => throw new KarnakException("no prediction made")
      case Some(v) => throw new KarnakException("value is of unexpected type: %s".format(v))
    }

    pred.intervalSecs = p.interval match {
      case Some(interval: Interval) => {
        (interval.upper.asInstanceOf[Number].longValue - interval.lower.asInstanceOf[Number].longValue) / 2
      }
      case None => 0
    }

    pred
  }

  private var predictors = Map[(String,String),UnsubmittedPredictor]()
	
  @throws(classOf[KarnakException])
  private def getPredictor(system: String, path:String, assignedWeights: Vector[Float] ): UnsubmittedPredictor = {
    if (!predictors.contains( (system, path) )) {
      predictors = predictors + ( (system, path) -> UnsubmittedPredictor.apply(system, path, assignedWeights))
    }
    predictors( system, path) 
  }


  @throws(classOf[KarnakException])
  def toCmlQuery(query: karnak.service.predict.UnsubmittedStartTimeQuery): Query = {
    var features = Vector[Option[Any]]()

    val queueState = GlueDatabase.readCurrentQueueState(query.system)
    if (queueState == null) {
      throw new KarnakException("failed to read current queue state for %s".format(query.system))
    }

    val queryJob = new karnak.service.Job()
    queryJob.id = "karnak-predict-job"
    queryJob.system = query.system
    queryJob.queue = query.queue
    queryJob.processors = query.processors
    queryJob.requestedWallTime = query.requestedWallTime
    queryJob.submitTime = queueState.time

    queueState.jobIds.add(queryJob.id)
    queueState.jobs.put(queryJob.id,queryJob)

    val featureNames = Vector[String](
      "queue",
      "processors",
      "requestedWallTime",
      "time",
      "countAhead",
      "workAhead",
      "countRunning",
      "processorsRunning",
      "workRunning",
      "countAheadQueue",
      "workAheadQueue",
      "countAheadOtherQueues",
      "workAheadOtherQueues",
      "countAheadLessEqualProcs",
      "workAheadLessEqualProcs",
      "countAheadLessEqualWork",
      "workAheadLessEqualWork",
      "waitTime"
    )

    val values = cmlValues(queryJob,queueState)
    new Query(featureNames.map(name => values(name)),query.intervalPercent)
  }
}




class UnsubmittedPredictor(name: String, schema: ExperienceSchema, path: String, weights: Vector[Float] ) extends WaitTimePredictor(name, schema, path, weights ) {
  protected def configFilePattern = """(unsubmitted-\d+.json)""".r
}


class BackTestUnsubmittedPredictor(name: String, schema: ExperienceSchema, path: String, weights: Vector[Float] ) extends WaitTimePredictor(name, schema, path, weights ) {
  protected def configFilePattern = """(unsubmitted-\d+.json)""".r

/* TODO: where do we get the simulatated current time to supply it with updatePredictorsIfNecessary() ?
  override def predict(query: Query): Try[Prediction] = {
    println("predict")
    print(query.toString(schema))

    updatePredictorsIfNecessary(Query.)

    for (predictor <- predictors) {
      predictor.predict(query) match {
        case Success(p) => printf("  %s: %s %s%n",predictor.name,p.features.last,p.interval)
        case Failure(e) => printf("  %s: no prediction: %s%n",predictor.name,e)
      }
    }

    super.predict(query)
  }
*/

}


/*********************************************************************************************************************/

object SubmittedPredictor extends PredictorUtils with Logging {
  def apply(name: String): SubmittedPredictor = {
    logger.info("creating submitted predictor for "+name)
    new SubmittedPredictor(name,dropTime(KarnakDatabase.getSubmittedSchema(name)), "/home/karnak/karnak/etc/predictors/v1/%s/".format(name), Vector(1,1,1,1,1))
  }
  

   def apply(name: String, path:String, assignedWeights: Vector[Float] ): SubmittedPredictor = {
    logger.info("creating unsubmitted predictor for "+name)
    new SubmittedPredictor(name,dropTime(KarnakDatabase.getUnsubmittedSchema(name)), path, assignedWeights)
  }


  @throws(classOf[KarnakException])
  def getPrediction(query: SubmittedStartTimeQuery, weightOption: WeightingEnum): SubmittedStartTimePrediction = {
    logger.info("predict")


    /* val predictor = getPredictor(query.system) */

    val predictor = weightOption match {
      case WeightingEnum.Bagging => getPredictor(query.system, "/home/karnak/karnak/etc/predictors/v1/%s/".format(query.system), Vector(1,1,1,1,1))
      case WeightingEnum.Single  => getPredictor(query.system, "/home/karnak/karnak/etc/predictors/v2/%s/".format(query.system), Vector(0,0,1,0,0)) 
    }

    val q = toCmlQuery(query)
    val p = predictor.predict(dropTime(q)) match {
      case Success(p) => p
      case Failure(e: PredictException) => {
        throw new KarnakException(e.getMessage())
      }
      case Failure(e) => {
        throw new KarnakException("predict failed: %s".format(e))
      }
    }

    val pred = new SubmittedStartTimePrediction(query)
    //pred.submitTime
    pred.processors = p.features(1) match {
      case Some(procs: Int) => procs
      case None => -1
      case Some(v) => throw new KarnakException("value is of unexpected type: %s".format(v))
    }
    pred.requestedWallTime = p.features(2) match {
      case Some(wallTime: Int) => wallTime
      case None => -1
      case Some(v) => throw new KarnakException("value is of unexpected type: %s".format(v))
    }

    val time = q.features(3) match {
      case Some(t: Long) => t
      case None => throw new KarnakException("didn't find a time to wait from")
      case Some(v) => throw new KarnakException("value is of unexpected type: %s".format(v))
    }
    pred.startTime = p.features(26) match {
      case Some(waitTime: Long) => new Date((time + waitTime) * 1000)
      case None => throw new KarnakException("no prediction made")
      case Some(v) => throw new KarnakException("value is of unexpected type: %s".format(v))
    }

    pred.intervalSecs = p.interval match {
      case Some(interval: Interval) => {
        (interval.upper.asInstanceOf[Number].longValue - interval.lower.asInstanceOf[Number].longValue) / 2
      }
      case None => 0
    }

    pred
  }


 private var predictors = Map[(String,String),SubmittedPredictor]()
  
  @throws(classOf[KarnakException])
  private def getPredictor(system: String, path:String, assignedWeights: Vector[Float] ): SubmittedPredictor = {
    if (!predictors.contains( (system, path) )) {
      predictors = predictors + ( (system, path) -> SubmittedPredictor.apply(system, path, assignedWeights))
    }
    predictors( system, path) 
  }



  @throws(classOf[KarnakException])
  def toCmlQuery(query: karnak.service.predict.SubmittedStartTimeQuery): Query = {
    var features = Vector[Option[Any]]()

    val queueState = GlueDatabase.readCurrentQueueState(query.system)
    if (queueState == null) {
      throw new KarnakException("failed to read current queue state for %s".format(query.system))
    }
    if (!queueState.jobs.containsKey(query.jobId)) {
      throw new KarnakException("job %s is not currently queued on %s".format(query.jobId,query.system))
    }
    val queryJob = queueState.jobs.get(query.jobId)

    val featureNames = Vector[String](
      "queue",
      "processors",
      "requestedWallTime",
      "time",
      "countAhead",
      "workAhead",
      "countRunning",
      "processorsRunning",
      "workRunning",
      "countAheadUser",
      "workAheadUser",
      "countRunningUser",
      "processorsRunningUser",
      "workRunningUser",
      "countAheadProject",
      "workAheadProject",
      "countRunningProject",
      "processorsRunningProject",
      "workRunningProject",
      "countAheadQueue",
      "workAheadQueue",
      "countAheadOtherQueues",
      "workAheadOtherQueues",
      "countAheadLessEqualProcs",
      "workAheadLessEqualProcs",
      "countAheadLessEqualWork",
      "workAheadLessEqualWork",
      "waitTime"
    )

    val values = cmlValues(queryJob,queueState)
    new Query(featureNames.map(name => values(name)),query.intervalPercent)
  }
}

class SubmittedPredictor(name: String, schema: ExperienceSchema, path: String, weights: Vector[Float] ) extends WaitTimePredictor(name, schema, path, weights ) {
  protected def configFilePattern = """(submitted-\d+.json)""".r
}

/*********************************************************************************************************************/

// name is the system name
//    extends cml.learn.ensemble.WeightedConfidencePredictor(name, schema) with Logging {
abstract class WaitTimePredictor(name: String, schema: ExperienceSchema, val pathPrefix: String, assignedWeights: Vector[Float]) 
    extends cml.learn.ensemble.WeightedPredictor(name, schema) with Logging {

  protected def configFilePattern: scala.util.matching.Regex

  override def predict(query: Query): Try[Prediction] = {
    println("predict")
    print(query.toString(schema))

    updatePredictorsIfNecessary()

    for (predictor <- predictors) {
      predictor.predict(query) match {
        case Success(p) => printf("  %s: %s %s%n",predictor.name,p.features.last,p.interval)
        case Failure(e) => printf("  %s: no prediction: %s%n",predictor.name,e)
      }
    }

    super.predict(query)
  }

  protected var lastUpdate = new Date(0)

  protected def updatePredictorsIfNecessary(now: Date= new Date() ): Unit = {
    /* val now = new Date() */
    if (now.getTime() > lastUpdate.getTime() + 60*1000) {
      updatePredictors()
      lastUpdate = now

    }
  }

  /* private val pathPrefix = "/home/karnak/karnak/etc/predictors/%s/".format(name) */
  weights = assignedWeights;

  private var lastPredUpdate = Map[String,FileTime]()

  private def updatePredictors(): Unit = synchronized {
    logger.info("updating predictors for "+name)
    val dir = new File(pathPrefix)
    val pattern = configFilePattern

    var configNames = Vector[String]()
    for (name <- dir.list()) {
      name match {
        case pattern(n) => configNames = configNames :+ name
        case _ => 
      }
    }

    val curPreds: Map[String,Predictor] = predictors.map(pred => (pred.name -> pred)).toMap

    val tryPreds = configNames.map(name => {
      val path = Paths.get(pathPrefix+name)
      val modifyTime = Files.getLastModifiedTime(path)
      if (curPreds.contains(name)) {
        if (modifyTime.toMillis() > lastPredUpdate(name).toMillis()) {
          // replace predictors with new config files
          logger.info("reading updated predictor "+name)
          lastPredUpdate = lastPredUpdate + (name -> modifyTime)
          IO.readTreePredictor(pathPrefix+name)
        } else {
          Success(curPreds(name))
        }
      } else {
        // add predictors when config files appear
        logger.info("reading new predictor "+name)
        lastPredUpdate = lastPredUpdate + (name -> modifyTime)
        IO.readTreePredictor(pathPrefix+name)
      }
      // predictors that don't have config files anymore are dropped
    })
    predictors = tryPreds.collect({case Success(pred) => pred})

    /* Jungha Woo 06/08/2016
      This is the place we can compute moving average.
      Give different weights to different window-size prediction, and
      compute the error rate. Error rate is the squared error sum of the prediction and the actual wait times.
      Currently equally weighted prediction is returned to the user ( querier)
      weights = predictors.map(pred => 1.0f)
    */
    
  }
}

/*********************************************************************************************************************/
