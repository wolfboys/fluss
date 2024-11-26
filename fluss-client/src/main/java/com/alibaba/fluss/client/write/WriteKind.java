/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.client.table.FlussTable;
import com.alibaba.fluss.row.InternalRow;

/** Lists all kinds of write operation. */
@Internal
public enum WriteKind {
    /** Write kind for {@link FlussTable#put(InternalRow)}. */
    PUT,
    /** Write kind for {@link FlussTable#delete(InternalRow)}. */
    DELETE,
    /** Write kind for {@link FlussTable#append(InternalRow)}. */
    APPEND
}