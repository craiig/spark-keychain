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
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockId, RDDBlockId}

/**
 * An identifier for a partition in an RDD.
 */
trait Partition extends Serializable {
  /**
   * Get the partition's index within its parent RDD
   */
  def index: Int

  /**
   * Get the partition's blockID given it's parent RDD
   * Important that this is static and calculated at partition
   * allocation time
   */
  // @transient def rdd: RDD[_] //kept only on the client
  //val blockId: BlockId = RDDBlockId(rdd.id, index)
  
  val blockId: Option[BlockId] = None
  /* called by the cache manager with an associated RDD in case a block ID has
   * not been set previously, used for backwards compatibility with old RDDs
   * with non unique partitions */
  def getBlockId(rdd: RDD[_]): BlockId = {
    blockId.getOrElse( RDDBlockId(rdd.id, index) )
  }

  // A better default implementation of HashCode
  override def hashCode(): Int = index
}
