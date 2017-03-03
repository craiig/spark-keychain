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

package org.apache.spark.mllib.rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.random.RandomDataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.storage.{BlockId, RDDUniqueBlockId}

import scala.reflect.ClassTag
import scala.util.Random

private[mllib] class RandomRDDPartition[T](
    val rdd: RDD[_],
    override val index: Int,
    val size: Int,
    val generator: RandomDataGenerator[T],
    val seed: Long) extends Partition {

  require(size >= 0, "Non-negative partition size required.")

  /* data is a function of the size, generator, and seed used
   * we don't even need to include the partition index */
  def uniquename:String = s"randomRDD_size=${size}_gen=${generator.getClass.getName}_seed=${seed}_${index}"

  override val blockId: BlockId = {
    RDDUniqueBlockId(uniquename)
  }
}

// These two classes are necessary since Range objects in Scala cannot have size > Int.MaxValue
private[mllib] class RandomRDD[T: ClassTag](sc: SparkContext,
    size: Long,
    numPartitions: Int,
    @transient private val rng: RandomDataGenerator[T],
    @transient private val seed: Long = Utils.random.nextLong) extends RDD[T](sc, Nil) {

  require(size > 0, "Positive RDD size required.")
  require(numPartitions > 0, "Positive number of partitions required")
  require(math.ceil(size.toDouble / numPartitions) <= Int.MaxValue,
    "Partition size cannot exceed Int.MaxValue")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[T] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition[T]]
    RandomRDD.getPointIterator[T](split)
  }

  override def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(this, size, numPartitions, rng, seed)
  }
}

private[mllib] class RandomVectorRDD(sc: SparkContext,
    size: Long,
    vectorSize: Int,
    numPartitions: Int,
    @transient private val rng: RandomDataGenerator[Double],
    @transient private val seed: Long = Utils.random.nextLong) extends RDD[Vector](sc, Nil) {

  require(size > 0, "Positive RDD size required.")
  require(numPartitions > 0, "Positive number of partitions required")
  require(vectorSize > 0, "Positive vector size required.")
  require(math.ceil(size.toDouble / numPartitions) <= Int.MaxValue,
    "Partition size cannot exceed Int.MaxValue")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[Vector] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition[Double]]
    RandomRDD.getVectorIterator(split, vectorSize)
  }

  override protected def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(this, size, numPartitions, rng, seed)
  }
}

private[mllib] object RandomRDD {

  def getPartitions[T](
      rdd: RDD[_],
      size: Long,
      numPartitions: Int,
      rng: RandomDataGenerator[T],
      seed: Long): Array[Partition] = {

    val partitions = new Array[RandomRDDPartition[T]](numPartitions)
    var i = 0
    var start: Long = 0
    var end: Long = 0
    val random = new Random(seed)
    while (i < numPartitions) {
      end = ((i + 1) * size) / numPartitions
      partitions(i) = new RandomRDDPartition(rdd, i, (end - start).toInt, rng, random.nextLong())
      start = end
      i += 1
    }
    partitions.asInstanceOf[Array[Partition]]
  }

  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getPointIterator[T: ClassTag](partition: RandomRDDPartition[T]): Iterator[T] = {
    val generator = partition.generator.copy()
    generator.setSeed(partition.seed)
    Iterator.fill(partition.size)(generator.nextValue())
  }

  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getVectorIterator(
      partition: RandomRDDPartition[Double],
      vectorSize: Int): Iterator[Vector] = {
    val generator = partition.generator.copy()
    generator.setSeed(partition.seed)
    Iterator.fill(partition.size)(new DenseVector(Array.fill(vectorSize)(generator.nextValue())))
  }
}
