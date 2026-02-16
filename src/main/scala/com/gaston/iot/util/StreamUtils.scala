package com.gaston.iot.util

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object StreamUtils {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Logs information in a streaming batch. Meant to be used as argument to forEachBatch in a streaming query def:
   * .forEachBatch(source _)
   * @param batch_df - The current batch Dataframe
   * @param batch_id - Batch Id
   */
  def log_batch(batch_df: DataFrame, batch_id: Long): Unit = {
    logger.debug(s"Batch Id: $batch_id, Row Count: ${batch_df.count()}")
    batch_df.show(truncate = false)
  }

}
