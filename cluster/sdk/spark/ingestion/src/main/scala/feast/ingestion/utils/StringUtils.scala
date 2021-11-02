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
package feast.ingestion.utils

import com.google.common.hash.Hashing

object StringUtils {
  private def suffixHash(expr: String): String = {
    Hashing.murmur3_32().hashBytes(expr.getBytes).toString
  }

  def trimAndHash(expr: String, maxLength: Int): String = {
    // Length 8 suffix as derived from murmurhash_32 implementation
    val maxPrefixLength = maxLength - 8
    if (expr.length > maxLength)
      expr
        .take(maxPrefixLength)
        .concat(suffixHash(expr.substring(maxPrefixLength)))
    else
      expr
  }
}
