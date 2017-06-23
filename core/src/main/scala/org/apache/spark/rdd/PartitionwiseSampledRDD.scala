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

package org.apache.spark.rdd

import java.util.Random

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.util.random.RandomSampler
import org.apache.spark.util.Utils
import org.apache.spark.storage.{BlockId, RDDBlockId, RDDUniqueBlockId}

private[spark]
class PartitionwiseSampledRDDPartition(@transient val rdd: RDD[_], val prev: Partition, val seed: Long)
  extends Partition with Serializable with Logging {
  override val index: Int = prev.index

  blockId = {
    val prevBlockID = prev.blockId
    if( !prevBlockID.isEmpty && prevBlockID.get.isInstanceOf[RDDUniqueBlockId] ){
      /* index is encoded by prevBlockId */
      val str = s"PartitionwiseSampledRDD{ seed:${seed}, prev:${prevBlockID.get} }"
      Some(RDDUniqueBlockId(str))
    } else {
      logInfo(s"PartitionwiseSampledRDDPartition falling back to standard block id: "
        //+ s"rdd parent: ${prev.rdd.getClass().getName()}"
        + s" callsite: ${rdd.getCreationSite}"
        )
      Some(RDDBlockId(rdd.id, index))
    }
  }
}

/**
 * An RDD sampled from its parent RDD partition-wise. For each partition of the parent RDD,
 * a user-specified [[org.apache.spark.util.random.RandomSampler]] instance is used to obtain
 * a random sample of the records in the partition. The random seeds assigned to the samplers
 * are guaranteed to have different values.
 *
 * @param prev RDD to be sampled
 * @param sampler a random sampler
 * @param preservesPartitioning whether the sampler preserves the partitioner of the parent RDD
 * @param seed random seed
 * @tparam T input RDD item type
 * @tparam U sampled RDD item type
 */
private[spark] class PartitionwiseSampledRDD[T: ClassTag, U: ClassTag](
    prev: RDD[T],
    sampler: RandomSampler[T, U],
    preservesPartitioning: Boolean,
    @transient private val seed: Long = Utils.random.nextLong)
  extends RDD[U](prev) {

  @transient override val partitioner = if (preservesPartitioning) prev.partitioner else None

  override def getPartitions: Array[Partition] = {
    val random = new Random(seed)
    firstParent[T].partitions.map(x => new PartitionwiseSampledRDDPartition(this, x, random.nextLong()))
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[PartitionwiseSampledRDDPartition].prev)

  override def compute(splitIn: Partition, context: TaskContext): Iterator[U] = {
    val split = splitIn.asInstanceOf[PartitionwiseSampledRDDPartition]
    val thisSampler = sampler.clone
    thisSampler.setSeed(split.seed)
    thisSampler.sample(firstParent[T].iterator(split.prev, context))
  }
}
