
/*
 * (C) Copyright IBM Corp. 2015 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 *  http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package src.main.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object pagerankApp {

  def main(args: Array[String]) {
    if (args.length < 5) {
      println("usage: <input> <output> <minEdge> <maxIterations> <tolerance> <resetProb> <StorageLevel>")
      System.exit(0)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf
    conf.setAppName("Spark PageRank Application")
    val sc = new SparkContext(conf)
    //conf.registerKryoClasses(Array(classOf[pagerankApp] ))

    val input = args(0)
    val output = args(1)
    val minEdge = args(2).toInt
    val maxIterations = args(3).toInt
    val tolerance = args(4).toDouble
    val resetProb = args(5).toDouble
    val storageLevel = args(6)

    var sl: StorageLevel = StorageLevel.MEMORY_ONLY;
    if (storageLevel == "MEMORY_AND_DISK_SER")
      sl = StorageLevel.MEMORY_AND_DISK_SER
    else if (storageLevel == "MEMORY_AND_DISK")
      sl = StorageLevel.MEMORY_AND_DISK

    val lines = sc.textFile(input)
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(_ => 1.0)

    for (_ <- 1 to maxIterations) {
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    ranks.coalesce(1).saveAsTextFile(output)
    sc.stop();
  }

}
