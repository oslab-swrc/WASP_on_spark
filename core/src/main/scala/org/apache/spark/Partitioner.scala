# SPDX-FileCopyrightText: Copyright 2021 Seoul National University
# We modified the original code to add functions for WASP
# SPDX-License-Identifier: Apache-2.0

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io._
import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.reflect.{ClassTag, classTag}
import scala.util.hashing.byteswap32

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{CollectionsUtils, Utils}
import org.apache.spark.util.random.{XORShiftRandom, SamplingUtils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * An object that defines how the elements in a key-value pair RDD are partitioned by key.
 * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
 */
abstract class Partitioner extends Serializable {
  def numPartitions: Int
  def getPartition(key: Any): Int
}

object Partitioner {
  /**
   * Choose a partitioner to use for a cogroup-like operation between a number of RDDs.
   *
   * If any of the RDDs already has a partitioner, choose that one.
   *
   * Otherwise, we use a default HashPartitioner. For the number of partitions, if
   * spark.default.parallelism is set, then we'll use the value from SparkContext
   * defaultParallelism, otherwise we'll use the max number of upstream partitions.
   *
   * Unless spark.default.parallelism is set, the number of partitions will be the
   * same as the number of partitions in the largest upstream RDD, as this should
   * be least likely to cause out-of-memory errors.
   *
   * We use two method parameters (rdd, others) to enforce callers passing at least 1 RDD.
   */
  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    val bySize = (Seq(rdd) ++ others).sortBy(_.partitions.size).reverse
    for (r <- bySize if r.partitioner.isDefined && r.partitioner.get.numPartitions > 0) {
      return r.partitioner.get
    }
    if (rdd.context.conf.contains("spark.default.parallelism")) {
      new HashPartitioner(rdd.context.defaultParallelism)
    } else {
      new HashPartitioner(bySize.head.partitions.size)
    }
  }
}

/**
 * A [[org.apache.spark.Partitioner]] that implements hash-based partitioning using
 * Java's `Object.hashCode`.
 *
 * Java arrays have hashCodes that are based on the arrays' identities rather than their contents,
 * so attempting to partition an RDD[Array[_]] or RDD[(Array[_], _)] using a HashPartitioner will
 * produce an unexpected or incorrect result.
 */
class HashPartitioner(partitions: Int) extends Partitioner with Logging {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

/**
 * A [[org.apache.spark.Partitioner]] that partitions sortable records by range into roughly
 * equal ranges. The ranges are determined by sampling the content of the RDD passed in.
 *
 * Note that the actual number of partitions created by the RangePartitioner might not be the same
 * as the `partitions` parameter, in the case where the number of sampled records is less than
 * the value of `partitions`.
 */
class RangePartitioner[K : Ordering : ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    private var ascending: Boolean = true)
  extends Partitioner with Logging {

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")
  

  private var ordering = implicitly[Ordering[K]]

  // An array of upper bounds for the first (partitions - 1) partitions
  private var rangeBounds: Array[K] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      val sampleSize = math.min(20.0 * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.size).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.size).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
     
        RangePartitioner.determineBounds(candidates, rdd)
      }
    }
  }

  def numPartitions: Int = rangeBounds.length + 1

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_, _] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeBoolean(ascending)
        out.writeObject(ordering)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ascending = in.readBoolean()
        ordering = in.readObject().asInstanceOf[Ordering[K]]
        binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}

private[spark] object RangePartitioner extends Logging {

  /**
   * Sketches the input RDD via reservoir sampling on each partition.
   *
   * @param rdd the input RDD to sketch
   * @param sampleSizePerPartition max sample size per partition
   * @return (total number of items, an array of (partitionId, number of items, sample))
   */
  def sketch[K : ClassTag](
      rdd: RDD[K],
      sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }

  /**
   * Determines the bounds for range partitioning from candidates with weights indicating how many
   * items each represents. Usually this is 1 over the probability used to sample this candidate.
   *
   * @param candidates unordered candidates with weights
   * @param rdd RDD
   * @return selected bounds
   */
  def determineBounds[K : Ordering : ClassTag](
      candidates: ArrayBuffer[(K, Float)],
      rdd: RDD[_]): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum

    // (BJH) Function to calculate optimal partition
    var (optLoP, optDoP) = PredictLoPDoP(candidates, rdd.context)
    rdd.context.setLoP(optLoP)
    rdd.context.setDoP(0)

    logError(s",Partitioner ARC LoP: $optLoP, DoP: $optDoP")

    val step = sumWeights / optLoP
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < optLoP - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    
    bounds.toArray
  }

  def getExecutorMemory(conf: SparkConf): Long = {
    val confMemory = conf.getOption("spark.executor.memory")
      .map(Utils.memoryStringToMb)

    val totalMemory = (confMemory match {
        case Some(x: Int) => x.toLong
        case _ => 1024
      })

    val reserved = 300
    val fraction = conf.getDouble("spark.memory.fraction", 0.75)

    ((totalMemory - reserved) * fraction).toLong
  }

  def PredictLoPDoP[K : Ordering : ClassTag](
      candidates: ArrayBuffer[(K, Float)],
      sc: SparkContext): (Int, Int) = {
    // (BJH) Get param to calculate optDoP
    val currentMem = getExecutorMemory(sc.conf)
    val size = candidates.length
    val weight = candidates(0)._2 / 1024 / 1024

    // (BJH) Need to get a, kvpair and gcconst
    val kvpair = 108
    val dataSize = (kvpair * size * weight * 13).toDouble
    
    val confExecutor = sc.conf.getOption("spark.total.executor.number")
    val totalExecutor = (confExecutor match {
      case Some(x: String) => x.toInt
      case _ => 1
    })

    val parama = math.log10(dataSize) - 1
    val param10a = math.pow(10, parama.toInt)
//    val gcconst = dataSize / param10a

//    val a = gcconst
    val b = dataSize
    val c = currentMem

    logError(s",ARC b: $b, c: $c")
    val confCore = sc.conf.getOption("spark.total.core.number")
    val calDoP = (confCore match {
        case Some(x: String) => x.toInt
        case _ => 1
      })
    var calLoP = math.round((b * calDoP) / c).toInt

    val log2LoP = math.round(math.log10(calLoP) / math.log10(2)).toInt
    var minLoP = math.pow(2, log2LoP).toInt
    var minDoP = calDoP.toInt

    // (BJH) New model in 2-d
    var a2 = dataSize / minLoP
    var b2 = (dataSize * minDoP) / (minLoP * currentMem)

    var minRound = {
      if (b2 < 1) { a2 } else { a2 * (1 + b2) }
    }

    var minStage = minRound * minLoP / (minDoP * totalExecutor)
    var minPenalty = b2

    var done = true
    var roundArr = Array.fill(8)(0.0)
    var stageArr = Array.fill(8)(0.0)
    var penaltyArr = Array.fill(8)(0.0)
    var index = 0
    var found = false
    var defaultDoP = minDoP
    var beforePenalty = minPenalty
    var count = 0

    while(done) {
      logError(",ARC initial: " + minRound + ", " + minStage + ", " + b2)
      for(i <- 0 to 7) {
        var temp = traverse2d(minLoP, minDoP, i)
        a2 = dataSize / temp._1
        b2 = (dataSize * temp._2) / (temp._1 * currentMem)
        roundArr(i) = {
          if (b2 < 1) { a2 } else { a2 * (1 + b2) }
        }

        stageArr(i) = roundArr(i) * temp._1 / (temp._2 * totalExecutor)
        penaltyArr(i) = b2
        logError(",ARC " + i + ": " + roundArr(i) + ", " + stageArr(i) + ", " + b2)
        if (b2 < 1 && b2 > 0.1) {
          found = true
          if (minRound > roundArr(i)) {
            index = i; minRound = roundArr(i);
            minStage = stageArr(i); minPenalty = penaltyArr(i)
          } else if (minRound == roundArr(i)) {
            if (minStage > stageArr(i)) {
              index = i; minRound = roundArr(i)
              minStage = stageArr(i); minPenalty = penaltyArr(i)
            } else if (minStage == stageArr(i)) {
              if (minPenalty < penaltyArr(i)) {
                index = i; minRound = roundArr(i)
                minStage = stageArr(i); minPenalty = penaltyArr(i)
              }
            }
          }
        }
      }

      if (found == false) {
        for(i <- 0 to 7) {
          if (minRound > roundArr(i)) {
            index = i; minRound = roundArr(i)
            minStage = stageArr(i); minPenalty = penaltyArr(i)
            done = true
          } else if (minRound == roundArr(i)) {
            if (minStage > stageArr(i)) {
              index = i; minRound = roundArr(i)
              minStage = stageArr(i); minPenalty = penaltyArr(i)
              done = true
            } else if (minStage == stageArr(i)) {
              if (minPenalty > penaltyArr(i)) {
                index = i; minRound = roundArr(i)
                minStage = stageArr(i); minPenalty = penaltyArr(i)
                done = true
              }
            }
          }
        }
      }

      if (beforePenalty == minPenalty) {
        count += 1
        if (count == 2) { done = false }
      }

      if (done == true) {
        beforePenalty = minPenalty
        var temp = traverse2d(minLoP, minDoP, index)
        minLoP = temp._1; minDoP = temp._2
        found = false
        logError(",ARC found: " + minLoP + ", " + minDoP + ", " + index)
        index = 0
      }
    }
    logError(",ARC end")

    if (minLoP > 128 * 8) {
      minLoP = 128
      minDoP = calDoP
    }

    return (minLoP, minDoP)
  }

  def traverse2d(lop: Int, dop: Int, index: Int): (Int, Int) = {
    var nlop = -1
    var ndop = -1
    index match {
      case 0 =>
        nlop = (lop / 2).toInt; ndop = (dop * 2).toInt
      case 1 =>
        nlop = lop.toInt; ndop = (dop * 2).toInt
      case 2 =>
        nlop = (lop * 2).toInt; ndop = (dop * 2).toInt
      case 3 =>
        nlop = (lop / 2).toInt; ndop = dop.toInt
      case 4 =>
        nlop = (lop * 2).toInt; ndop = dop.toInt
      case 5 =>
        nlop = (lop / 2).toInt; ndop = (dop / 2).toInt
      case 6 =>
        nlop = lop.toInt; ndop = (dop / 2).toInt
      case 7 =>
        nlop = (lop * 2).toInt; ndop = (dop / 2).toInt
      case _ =>
        nlop = -1; ndop = -1
    }

    if (nlop < 1) nlop = 1
    if (ndop < 1) ndop = 1

    return (nlop, ndop)
  }
}

