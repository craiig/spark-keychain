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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext, SparkContext}
import org.apache.commons.codec.binary.Base64
import org.apache.spark.storage.{BlockId, RDDUniqueBlockId}

private[spark] class MapPartition(val prev: Partition, val funcStr:String)
  extends Partition {
  override val index: Int = prev.index
  override def blockId(rdd:RDD[_]): BlockId = {
    /* how does this interact with the partitioner? */
   val str = s"${prev.blockId(rdd)}_map_${funcStr}"
   RDDUniqueBlockId(str) 
  }
}

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

    val f_serialized = {
      val ser = SparkContext.getOrCreate().env.serializer.newInstance()
      val funcBytes = ser.serialize(f)
      val funcStr = Base64.encodeBase64String(funcBytes.array)
      logInfo(s"serialized function of size ${funcBytes.limit} bytes, base64 encoded is ${funcStr.length} bytes")
      funcStr
    }

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions.map( p => new MapPartition(p, f_serialized) )

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split.asInstanceOf[MapPartition].prev, context))

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
