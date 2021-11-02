/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.metrics

import com.codahale.metrics.Gauge

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

class AtomicLongGauge(initialValue: Long = 0L) extends Gauge[Long] {
  val value                   = new AtomicLong(initialValue)
  override def getValue: Long = value.get()
}

class AtomicIntegerGauge(initialValue: Int = 0) extends Gauge[Int] {
  val value                  = new AtomicInteger(initialValue)
  override def getValue: Int = value.get()
}
