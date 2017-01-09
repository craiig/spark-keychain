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
import org.apache.spark.storage.{BlockId, RDDBlockId, RDDUniqueBlockId}

private[spark] class MapPartition(val prev: Partition, val funcStr:Option[String])
  extends Partition {
  override val index: Int = prev.index
  override def blockId(rdd:RDD[_]): BlockId = {
   //if the funcStr was successfully hashed, use that hash
   if (!funcStr.isEmpty){
     val str = s"map { ${prev.blockId(rdd)}, ${funcStr.get}}"
     RDDUniqueBlockId(str) 
   } else {
     RDDBlockId(rdd.id, index)
   }
  }
}

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
    lambdaHash: Option[String] = None)
  extends RDD[U](prev) {

  val f_outerHash:Option[String] = SparkContext.getOrCreate().hash(f)

  //only calculate a function hash if we were passed an innerhash
  val f_hash:Option[String] = if(!lambdaHash.isEmpty){
    Some(s"inner_${lambdaHash.get}_outer_${f_outerHash.get}")
  } else {
    println("MapPartition innerhash was empty so fell back to clasic naming")
    None
  }

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions.map( p => new MapPartition(p, f_hash) )

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split.asInstanceOf[MapPartition].prev, context))

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
