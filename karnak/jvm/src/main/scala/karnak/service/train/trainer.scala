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

import scala.util.{Success,Failure}

import java.io.File
import java.nio.file.{Paths,Files}
import java.util.Date
import java.util.{Timer,TimerTask}

import com.weiglewilczek.slf4s.Logging

import cml.learn._
import cml.learn.tree._

import org.apache.commons.daemon.{Daemon,DaemonContext}

/*********************************************************************************************************************/

object MetaTrainer extends Logging {
  /*
  // do this when evaluating metaparameters
  def validate(system: String, earliestTime: Date): Predictor = {
    val exps = KarnakDatabase.readSubmittedExperiences(system,earliestTime)
    printf("found %d experiences%n",exps.size)

    val validator = new cml.learn.CrossValidate()
    val trainer = createTrainer(exps(0).schema)
    val stats = validator.validate(trainer,exps,10)
    print(stats)

    val predictor = trainer.train(exps)
    //test(predictor,testExps)
    print(predictor)
    //print(predictor.stats)

    predictor
  }

  def test(predictor: Predictor, experiences: Vector[Experience]): Unit = {
    val start = new java.util.Date()
    var count = 0
    for (exp <- experiences) {
      val query = Query(exp) // default levelOfConfidence is 90, which is fine
      predictor.predict(query) match {
        case Success(pred) => //print(pred.toString)
        case Failure(error) => //print(error.toString)
      }
      count += 1
      if (count % 1000 == 0) {
        println("  at item %d".format(count))
      }
    }
    val end = new java.util.Date()
    println("predicted in %d seconds".format((end.getTime() - start.getTime()) / 1000))
  }
   */
}

/*********************************************************************************************************************/

import org.apache.log4j.PropertyConfigurator; // for the hack

object Trainer {

  // hack for now because of sbt difficulties
  PropertyConfigurator.configure("/home/karnak/karnak/etc/log4j.properties");

  def main(args: Array[String]): Unit = {
    val trainer = new Trainer()
    trainer.run()
  }
}

class Trainer extends Daemon with Logging {

  val SECOND = 1000l
  val MINUTE = 60*SECOND
  val HOUR = 60*MINUTE
  val DAY = 24*HOUR
  val WEEK = 7*DAY

  var timer = new Timer()
  var systems = Vector[String]()

  def init(context: DaemonContext): Unit = {
  }

  def start(): Unit = {
    schedule()
    timer.schedule(new UpdateTrainersTask(),DAY)
  }

  def stop(): Unit = {
    timer.cancel()
  }

  def destroy(): Unit = {
  }

  // interactive
  def run(): Unit = {
    while (true) {
      schedule()
      Thread.sleep(DAY)
    }
  }

  class UpdateTrainersTask() extends TimerTask {
    def run(): Unit = {
      schedule()
      timer.schedule(new UpdateTrainersTask(),DAY) // update systems once a day
    }
  }

  private def schedule(): Unit = {
    // recreate timers to pick up new systems
    timer.cancel()
    timer = new Timer()
    updateSystemNames()

    for (system <- systems) {
      timer.schedule(new UnsubmittedTrainTask(system,4*HOUR),0,MINUTE)
      timer.schedule(new UnsubmittedTrainTask(system,DAY),0,15*MINUTE)
      timer.schedule(new UnsubmittedTrainTask(system,WEEK),0,HOUR)
      timer.schedule(new UnsubmittedTrainTask(system,4*WEEK),0,HOUR)
      timer.schedule(new UnsubmittedTrainTask(system,8*WEEK),0,HOUR)

      timer.schedule(new SubmittedTrainTask(system,4*HOUR),0,MINUTE)
      timer.schedule(new SubmittedTrainTask(system,DAY),0,15*MINUTE)
      timer.schedule(new SubmittedTrainTask(system,WEEK),0,HOUR)
      timer.schedule(new SubmittedTrainTask(system,4*WEEK),0,HOUR)
      timer.schedule(new SubmittedTrainTask(system,8*WEEK),0,HOUR)
    }
  }

  private def updateSystemNames(): Unit = {
    logger.info("updating system names")
    systems = KarnakDatabase.getSystemNames().map(symbol => symbol.name).toVector
    for (system <- systems) {
      createDirectory(system)
    }
  }

  private def createDirectory(system: String): Unit = {
    val path = Paths.get("/home/karnak/karnak/etc/predictors/"+system)
    if (!Files.exists(path)) {
      logger.info("creating predictor configuration directory for "+system)
      Files.createDirectories(path)
    }
  }

  class UnsubmittedTrainTask(val system: String, val history: Long) extends TimerTask {
    val predName = "unsubmitted-%d.json".format(history)
    val path = "/home/karnak/karnak/etc/predictors/%s/".format(system,history)+predName

    def run(): Unit = {
      val now = new Date()
      val earliestTime = new Date(now.getTime()-history)

      if (!shouldTrain(path,history)) {
        return
      }
      logger.info("training unsubmitted for %s starting at %s".format(system,earliestTime))

      val exps = dropTime(KarnakDatabase.readUnsubmittedExperiences(system,earliestTime))
      val schema = dropTime(KarnakDatabase.getUnsubmittedSchema(system)).withCategories(exps)

      logger.debug("%d experiences".format(exps.size))
      exps.size match {
        case 0 => {
          logger.warn("can't train predictor: no experiences to train with")
          writeBlank(path)
        }
        case _ => {
          val trainer = createTrainer(predName,schema)
          trainer.train(exps) match {
            case Success(predictor) => IO.write(predictor,path)
            case Failure(e) => {
              logger.warn("can't train predictor: %s".format(e))
              writeBlank(path)
            }
          }
        }
      }
    }

    private def createTrainer(name: String, schema: ExperienceSchema): TreeTrainer = {
      val loss = new cml.learn.SquaredErrorLoss()
      val maxDepth=Int.MaxValue
      val minLossFraction=0.01f
      val minNodeSize=10
      val builder = new OneNodePerCategoryBuilder(loss,maxDepth,minLossFraction,minNodeSize,schema)

      val pruneFraction = 0.0f

      new TreeTrainer(name,schema,builder,pruneFraction)
    }
  }

  class SubmittedTrainTask(val system: String, val history: Long) extends TimerTask {
    val predName = "submitted-%d.json".format(history)
    val path = "/home/karnak/karnak/etc/predictors/%s/".format(system,history)+predName

    def run(): Unit = {
      val now = new Date()
      val earliestTime = new Date(now.getTime()-history)

      if (!shouldTrain(path,history)) {
        return
      }
      logger.info("training submitted for %s starting at %s".format(system,earliestTime))

      val exps = dropTime(KarnakDatabase.readSubmittedExperiences(system,earliestTime))
      val schema = dropTime(KarnakDatabase.getSubmittedSchema(system)).withCategories(exps)

      logger.debug("%d experiences".format(exps.size))
      exps.size match {
        case 0 => {
          logger.warn("can't train predictor: no experiences to train with")
          writeBlank(path)
        }
        case _ => {
          val trainer = createTrainer(predName,schema)
          trainer.train(exps) match {
            case Success(predictor) => IO.write(predictor,path)
            case Failure(e) => {
              logger.warn("can't train predictor: %s".format(e))
              writeBlank(path)
            }
          }
        }
      }
    }

    private def createTrainer(name: String, schema: ExperienceSchema): TreeTrainer = {
      val loss = new cml.learn.SquaredErrorLoss()
      val maxDepth=Int.MaxValue
      val minLossFraction=0.01f
      val minNodeSize=10
      val builder = new OneNodePerCategoryBuilder(loss,maxDepth,minLossFraction,minNodeSize,schema)

      val pruneFraction = 0.0f

      new TreeTrainer(name,schema,builder,pruneFraction)
    }
  }

  private def shouldTrain(path: String, history: Long): Boolean = {
    val file = new File(path)
    file.exists() match {
      case false => true
      case true => {
        val now = new Date()
        now.getTime() - file.lastModified > history / 10 match {
          case true => true
          case false => false
        }
      }
    }
  }

  // the time feature doesn't make any sense when using regression trees
  private def dropTime(schema: ExperienceSchema): ExperienceSchema = {
    val timeIndex = 3
    val s = new ExperienceSchema(schema.name)
    val (head,tail) = schema.features.splitAt(timeIndex)
    s.features = head ++ tail.tail
    s
  }

  private def dropTime(exps: Vector[Experience]): Vector[Experience] = {
    exps.size match {
      case 0 => exps
      case _ => {
        val timeIndex = 3
        exps.map(exp => {
          val (head,tail) = exp.features.splitAt(timeIndex)
          new Experience(head ++ tail.tail)
        })
      }
    }
  }

  // a blank file is used to say what time the trainer tried to create a predictor
  private def writeBlank(path: String): Unit = {
    val writer = new java.io.BufferedWriter(new java.io.FileWriter(path))
    writer.close()
  }

}

/*********************************************************************************************************************/
