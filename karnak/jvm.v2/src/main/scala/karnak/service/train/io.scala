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

import scala.util.{Try,Success,Failure}

import com.weiglewilczek.slf4s._

import spray.json._

import cml.learn._
import cml.learn.tree._

import karnak.KarnakException

/*********************************************************************************************************************/
/* 
https://github.com/spray/spray-json
JsonProtocol is nothing but a bunch of implicit values of type JsonFormat[T], whereby each JsonFormat[T] contains
the logic of how to convert instance of T to and from JSON. 
All default converters in the DefaultJsonProtocol producing JSON objects or arrays are actually implemented as
RootJsonFormat. When "manually" implementing a JsonFormat for a custom type T (rather than relying on case class support) 
you should think about whether you'd like to use instances of T as JSON document roots and choose between a 
"plain" JsonFormat and a RootJsonFormat accordingly.
*/
object KarnakJsonProtocol extends DefaultJsonProtocol {

  implicit object CategoryFeatureSchemaJsonFormat extends RootJsonFormat[CategoryFeatureSchema] {
    def write(schema: CategoryFeatureSchema) =
      JsObject("type" -> JsString("category"),
               "name" -> JsString(schema.name),
               "required" -> JsBoolean(schema.required),
               "categories" -> JsArray(schema.categories.toVector.map(category => JsString(category.name)))
      )

    def read(value: JsValue) =
      value.asJsObject.fields("type").convertTo[String] match {
        case "category" => {
          val categories = value.asJsObject.fields("categories").convertTo[JsArray].elements.map(catStr =>
            Symbol(catStr.convertTo[String])).toSet
          new CategoryFeatureSchema(Symbol(value.asJsObject.fields("name").convertTo[String]),
                                    value.asJsObject.fields("required").convertTo[Boolean],
                                    categories)
        }
        case _ => throw new KarnakException("expecting a type of 'category'")
      }
  }

  implicit object StringFeatureSchemaJsonFormat extends RootJsonFormat[StringFeatureSchema] {
    def write(schema: StringFeatureSchema) =
      JsObject("type" -> JsString("string"),
               "name" -> JsString(schema.name),
               "required" -> JsBoolean(schema.required),
               "maxLength" -> JsNumber(schema.maxLength)
      )

    def read(value: JsValue) =
      value.asJsObject.fields("type").convertTo[String] match {
        case "string" => new StringFeatureSchema(Symbol(value.asJsObject.fields("name").convertTo[String]),
                                                 value.asJsObject.fields("required").convertTo[Boolean],
                                                 value.asJsObject.fields("maxLength").convertTo[Int])
        case _ => throw new KarnakException("expecting a type of 'string'")
      }
  }

  implicit object BooleanFeatureSchemaJsonFormat extends RootJsonFormat[BooleanFeatureSchema] {
    def write(schema: BooleanFeatureSchema) =
      JsObject("type" -> JsString("boolean"),
               "name" -> JsString(schema.name),
               "required" -> JsBoolean(schema.required)
      )

    def read(value: JsValue) =
      value.asJsObject.fields("type").convertTo[String] match {
        case "boolean" => new BooleanFeatureSchema(Symbol(value.asJsObject.fields("name").convertTo[String]),
                                                   value.asJsObject.fields("required").convertTo[Boolean])
        case _ => throw new KarnakException("expecting a type of 'string'")
      }
  }

  implicit object IntFeatureSchemaJsonFormat extends RootJsonFormat[IntFeatureSchema] {
    def write(schema: IntFeatureSchema) =
      JsObject("type" -> JsString("int"),
               "name" -> JsString(schema.name),
               "required" -> JsBoolean(schema.required)
      )

    def read(value: JsValue) =
      value.asJsObject.fields("type").convertTo[String] match {
        case "int" => new IntFeatureSchema(Symbol(value.asJsObject.fields("name").convertTo[String]),
                                           value.asJsObject.fields("required").convertTo[Boolean])
        case _ => throw new KarnakException("expecting a type of 'int'")
      }
  }

  implicit object LongFeatureSchemaJsonFormat extends RootJsonFormat[LongFeatureSchema] {
    def write(schema: LongFeatureSchema) =
      JsObject("type" -> JsString("long"),
               "name" -> JsString(schema.name),
               "required" -> JsBoolean(schema.required)
      )

    def read(value: JsValue) =
      value.asJsObject.fields("type").convertTo[String] match {
        case "long" => new LongFeatureSchema(Symbol(value.asJsObject.fields("name").convertTo[String]),
                                             value.asJsObject.fields("required").convertTo[Boolean])
        case _ => throw new KarnakException("expecting a type of 'long'")
      }
  }

  implicit object FloatFeatureSchemaJsonFormat extends RootJsonFormat[FloatFeatureSchema] {
    def write(schema: FloatFeatureSchema) =
      JsObject("type" -> JsString("float"),
               "name" -> JsString(schema.name),
               "required" -> JsBoolean(schema.required)
      )

    def read(value: JsValue) =
      value.asJsObject.fields("type").convertTo[String] match {
        case "float" => new FloatFeatureSchema(Symbol(value.asJsObject.fields("name").convertTo[String]),
                                               value.asJsObject.fields("required").convertTo[Boolean])
        case _ => throw new KarnakException("expecting a type of 'float'")
      }
  }

  implicit object DoubleFeatureSchemaJsonFormat extends RootJsonFormat[DoubleFeatureSchema] {
    def write(schema: DoubleFeatureSchema) =
      JsObject("type" -> JsString("double"),
               "name" -> JsString(schema.name),
               "required" -> JsBoolean(schema.required)
      )

    def read(value: JsValue) =
      value.asJsObject.fields("type").convertTo[String] match {
        case "double" => new DoubleFeatureSchema(Symbol(value.asJsObject.fields("name").convertTo[String]),
                                                 value.asJsObject.fields("required").convertTo[Boolean])
        case _ => throw new KarnakException("expecting a type of 'double'")
      }
  }

  implicit object DateFeatureSchemaJsonFormat extends RootJsonFormat[DateFeatureSchema] {
    def write(schema: DateFeatureSchema) =
      JsObject("type" -> JsString("date"),
               "name" -> JsString(schema.name),
               "required" -> JsBoolean(schema.required),
               "dateFormat" -> JsString(schema.format.toPattern())
      )

    def read(value: JsValue) =
      value.asJsObject.fields("type").convertTo[String] match {
        case "date" => new DateFeatureSchema(Symbol(value.asJsObject.fields("name").convertTo[String]),
                                             value.asJsObject.fields("required").convertTo[Boolean],
                                             value.asJsObject.fields("dateFormat").convertTo[String])
        case _ => throw new KarnakException("expecting a type of 'date'")
      }
  }

  implicit object FeatureSchemaJsonFormat extends RootJsonFormat[FeatureSchema] {
    def write(schema: FeatureSchema) =
      schema match {
        case s: CategoryFeatureSchema => s.toJson
        case s: StringFeatureSchema => s.toJson
        case s: BooleanFeatureSchema => s.toJson
        case s: IntFeatureSchema => s.toJson
        case s: LongFeatureSchema => s.toJson
        case s: FloatFeatureSchema => s.toJson
        case s: DoubleFeatureSchema => s.toJson
        case s: DateFeatureSchema => s.toJson
        case _ => throw new KarnakException("unknown feature schema")
      }
    def read(value: JsValue) =
      value.asJsObject.fields("type").convertTo[String] match {
        case "category" => value.convertTo[CategoryFeatureSchema]
        case "string" => value.convertTo[StringFeatureSchema]
        case "boolean" => value.convertTo[BooleanFeatureSchema]
        case "int" => value.convertTo[IntFeatureSchema]
        case "long" => value.convertTo[LongFeatureSchema]
        case "float" => value.convertTo[FloatFeatureSchema]
        case "double" => value.convertTo[DoubleFeatureSchema]
        case "date" => value.convertTo[DateFeatureSchema]
        case _ => throw new KarnakException("unknown feature schema type "+value.asJsObject.fields("type"))
      }
  }

  implicit object ExperienceSchemaJsonFormat extends RootJsonFormat[ExperienceSchema] {
    def write(schema: ExperienceSchema) =
      JsObject("name" -> JsString(schema.name),
               "features" -> JsArray(schema.features.map(feature => feature.toJson)))
    def read(value: JsValue) = {
      val schema = new ExperienceSchema(value.asJsObject.fields("name").convertTo[String])
      schema.features = value.asJsObject.fields("features").convertTo[JsArray].elements.map(fval => fval.convertTo[FeatureSchema])
      schema
    }
  }


  implicit object DistributionJsonFormat extends RootJsonFormat[Distribution] {
    def write(distribution: Distribution) =
      distribution match {
        case d: GaussianDistribution => JsObject("type" -> JsString("gaussian"),
                                                 "mean" -> JsNumber(d.mean),
                                                 "stdDev" -> JsNumber(d.stdDev),
                                                 "goodness" -> JsNumber(d.goodness),
                                                 "size" -> JsNumber(d.size))
        case d: LogNormalDistribution => JsObject("type" -> JsString("lognormal"),
                                                  "location" -> JsNumber(d.location),
                                                  "scale" -> JsNumber(d.scale),
                                                  "goodness" -> JsNumber(d.goodness),
                                                  "size" -> JsNumber(d.size))
        case _ => throw new KarnakException("unknown type of distribution")
      }
    def read(value: JsValue) = {
      value.asJsObject.fields("type").convertTo[String] match {
        case "gaussian" => new GaussianDistribution(value.asJsObject.fields("mean").convertTo[Float],
                                                    value.asJsObject.fields("stdDev").convertTo[Float],
                                                    value.asJsObject.fields("goodness").convertTo[Float],
                                                    value.asJsObject.fields("size").convertTo[Int])
        case "lognormal" => new LogNormalDistribution(value.asJsObject.fields("location").convertTo[Float],
                                                      value.asJsObject.fields("scale").convertTo[Float],
                                                      value.asJsObject.fields("goodness").convertTo[Float],
                                                      value.asJsObject.fields("size").convertTo[Int])
      }
    }
  }

  implicit object LeafNodeJsonFormat extends RootJsonFormat[LeafNode] {
    def write(node: LeafNode) =
      JsObject("type" -> JsString("leaf"),
               "distribution" -> node.distribution.toJson)
    def read(value: JsValue) = {
      new LeafNode(value.asJsObject.fields("distribution").convertTo[Distribution])
    }
  }

  implicit object CategoryEdgeJsonFormat extends RootJsonFormat[CategoryEdge] {
    def write(edge: CategoryEdge) =
      JsObject("type" -> JsString("category"),
               "feature" -> JsNumber(edge.feature),
               "categories" -> JsArray(edge.categories.toVector.map(symbol => JsString(symbol.name))))
    def read(value: JsValue) = {
      value.asJsObject.fields("type").convertTo[String] match {
        case "category" => {
          new CategoryEdge(value.asJsObject.fields("feature").convertTo[Int],
            value.asJsObject.fields("categories").convertTo[JsArray].elements.map(jsStr => Symbol(jsStr.convertTo[String])).toSet)
        }
        case t => throw new KarnakException("expected a 'category' edge, not a "+t)
      }
    }
  }

  implicit object BooleanEdgeJsonFormat extends RootJsonFormat[BooleanEdge] {
    def write(edge: BooleanEdge) =
      JsObject("type" -> JsString("boolean"),
               "feature" -> JsNumber(edge.feature),
               "bool" -> JsBoolean(edge.bool))
    def read(value: JsValue) = {
      value.asJsObject.fields("type").convertTo[String] match {
        case "boolean" => {
          new BooleanEdge(value.asJsObject.fields("feature").convertTo[Int],
                          value.asJsObject.fields("bool").convertTo[Boolean])
        }
        case t => throw new KarnakException("expected a 'boolean' edge, not a "+t)
      }
    }
  }

  implicit object IntEdgeJsonFormat extends RootJsonFormat[IntEdge] {
    def write(edge: IntEdge) =
      JsObject("type" -> JsString("int"),
               "feature" -> JsNumber(edge.feature),
               "min" -> JsNumber(edge.min),
               "max" -> JsNumber(edge.max))
    def read(value: JsValue) = {
      value.asJsObject.fields("type").convertTo[String] match {
        case "int" => {
          new IntEdge(value.asJsObject.fields("feature").convertTo[Int],
                      value.asJsObject.fields("min").convertTo[Int],
                      value.asJsObject.fields("max").convertTo[Int])
        }
        case t => throw new KarnakException("expected a 'int' edge, not a "+t)
      }
    }
  }

  implicit object LongEdgeJsonFormat extends RootJsonFormat[LongEdge] {
    def write(edge: LongEdge) =
      JsObject("type" -> JsString("long"),
               "feature" -> JsNumber(edge.feature),
               "min" -> JsNumber(edge.min),
               "max" -> JsNumber(edge.max))
    def read(value: JsValue) = {
      value.asJsObject.fields("type").convertTo[String] match {
        case "long" => {
          new LongEdge(value.asJsObject.fields("feature").convertTo[Int],
                       value.asJsObject.fields("min").convertTo[Long],
                       value.asJsObject.fields("max").convertTo[Long])
        }
        case t => throw new KarnakException("expected a 'long' edge, not a "+t)
      }
    }
  }

  implicit object FloatEdgeJsonFormat extends RootJsonFormat[FloatEdge] {
    def write(edge: FloatEdge) =
      JsObject("type" -> JsString("float"),
               "feature" -> JsNumber(edge.feature),
               "min" -> JsNumber(edge.min),
               "max" -> JsNumber(edge.max))
    def read(value: JsValue) = {
      value.asJsObject.fields("type").convertTo[String] match {
        case "float" => {
          new FloatEdge(value.asJsObject.fields("feature").convertTo[Int],
                        value.asJsObject.fields("min").convertTo[Float],
                        value.asJsObject.fields("max").convertTo[Float])
        }
        case t => throw new KarnakException("expected a 'float' edge, not a "+t)
      }
    }
  }

  implicit object DoubleEdgeJsonFormat extends RootJsonFormat[DoubleEdge] {
    def write(edge: DoubleEdge) =
      JsObject("type" -> JsString("double"),
               "feature" -> JsNumber(edge.feature),
               "min" -> JsNumber(edge.min),
               "max" -> JsNumber(edge.max))
    def read(value: JsValue) = {
      value.asJsObject.fields("type").convertTo[String] match {
        case "double" => {
          new DoubleEdge(value.asJsObject.fields("feature").convertTo[Int],
                         value.asJsObject.fields("min").convertTo[Double],
                         value.asJsObject.fields("max").convertTo[Double])
        }
        case t => throw new KarnakException("expected a 'double' edge, not a "+t)
      }
    }
  }

  implicit object NoValueEdgeJsonFormat extends RootJsonFormat[NoValueEdge] {
    def write(edge: NoValueEdge) =
      JsObject("type" -> JsString("novalue"),
               "feature" -> JsNumber(edge.feature))
    def read(value: JsValue) = {
      value.asJsObject.fields("type").convertTo[String] match {
        case "novalue" => {
          new NoValueEdge(value.asJsObject.fields("feature").convertTo[Int])
        }
        case t => throw new KarnakException("expected a 'novalue' edge, not a "+t)
      }
    }
  }


  implicit object EdgeJsonFormat extends RootJsonFormat[Edge] {
    def write(edge: Edge) =
      edge match {
        case e: CategoryEdge => e.toJson
        case e: BooleanEdge => e.toJson
        case e: IntEdge => e.toJson
        case e: LongEdge => e.toJson
        case e: FloatEdge => e.toJson
        case e: DoubleEdge => e.toJson
        // DateEdge
        case e: NoValueEdge => e.toJson
      }

    def read(value: JsValue) = {
      value.asJsObject.fields("type").convertTo[String] match {
        case "category" => value.convertTo[CategoryEdge]
        case "boolean" => value.convertTo[BooleanEdge]
        case "int" => value.convertTo[IntEdge]
        case "long" => value.convertTo[LongEdge]
        case "float" => value.convertTo[FloatEdge]
        case "double" => value.convertTo[DoubleEdge]
        case "novalue" => value.convertTo[NoValueEdge]
        case _ => throw new KarnakException("unknown tree edge type "+value.asJsObject.fields("type"))
      }
    }
  }

    //"children" -> JsArray(node.children.map(edgeNode => edgeNode.toJson)))

  implicit object InternalNodeJsonFormat extends RootJsonFormat[InternalNode] {
    def write(node: InternalNode) =
      JsObject("type" -> JsString("internal"),
               "distribution" -> node.distribution.toJson,
               "children" -> JsArray(node.children.map(edgeNode => EdgeNodeJsonFormat.write(edgeNode))))
    def read(value: JsValue) = {
      new InternalNode(value.asJsObject.fields("distribution").convertTo[Distribution],
        value.asJsObject.fields("children").convertTo[JsArray].elements.map(jsv => jsv.convertTo[(Edge,Node)])
      )
    }
  }

  implicit object NodeJsonFormat extends RootJsonFormat[Node] {
    def write(node: Node) =
      node match {
        case n: LeafNode => n.toJson
        case n: InternalNode => n.toJson
      }

    def read(value: JsValue) = {
      value.asJsObject.fields("type").convertTo[String] match {
        case "leaf" => value.convertTo[LeafNode]
        case "internal" => value.convertTo[InternalNode]
        case _ => throw new KarnakException("unknown tree node type "+value.asJsObject.fields("type"))
      }
    }
  }

  implicit object EdgeNodeJsonFormat extends RootJsonFormat[(Edge,Node)] {
    def write(edgeNode: (Edge,Node)) =
      JsArray(Vector(edgeNode._1.toJson,edgeNode._2.toJson))
    def read(value: JsValue) = {
      (value.convertTo[JsArray].elements(0).convertTo[Edge],value.convertTo[JsArray].elements(1).convertTo[Node])
    }
  }

  implicit object TreePredictorJsonFormat extends RootJsonFormat[TreePredictor] {
    def write(predictor: TreePredictor) =
      JsObject("name" -> JsString(predictor.name),
               "schema" -> predictor.schema.toJson,
               "tree" -> predictor.tree.toJson)
    def read(value: JsValue) = {
      new TreePredictor(value.asJsObject.fields("name").convertTo[String],
                        value.asJsObject.fields("schema").convertTo[ExperienceSchema],
                        value.asJsObject.fields("tree").convertTo[Node])
    }
  }

}

import KarnakJsonProtocol._

/*********************************************************************************************************************/

object IO extends Logging {
  def write(predictor: Predictor, path: String): Unit = {
    val ast = predictor match {
      case p: TreePredictor => p.toJson
      case _ => throw new KarnakException("can't write predictor of this type")
    }
    //println(ast.prettyPrint)
    val writer = new java.io.BufferedWriter(new java.io.FileWriter(path))
    writer.write(ast.prettyPrint)
    writer.close()
  }

  def readTreePredictor(path: String): Try[TreePredictor] = {
    try {
      val pred = scala.io.Source.fromFile(path).mkString.parseJson.convertTo[TreePredictor]
      Success(pred)
    } catch {
      case e: Exception => {
        logger.warn("failed to read predictor")
        Failure(e)
      }
    }
  }

}

/*********************************************************************************************************************/
