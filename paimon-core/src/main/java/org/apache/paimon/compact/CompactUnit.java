/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.compact;

import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;

import java.util.ArrayList;
import java.util.List;

/** A files unit for compaction. */
public interface CompactUnit {

    int outputLevel();

    List<DataFileMeta> files();

    List<Path> externalPaths();

    static CompactUnit fromLevelRuns(int outputLevel, List<LevelSortedRun> runs) {
        return fromLevelRuns(outputLevel, runs, new ArrayList<>());
    }

    static CompactUnit fromLevelRuns(
            int outputLevel, List<LevelSortedRun> runs, List<Path> externalPaths) {
        List<DataFileMeta> files = new ArrayList<>();
        for (LevelSortedRun run : runs) {
            files.addAll(run.run().files());
        }
        return fromFiles(outputLevel, files, externalPaths);
    }

    static CompactUnit fromFiles(
            int outputLevel, List<DataFileMeta> files, List<Path> externalPaths) {
        return new CompactUnit() {
            @Override
            public int outputLevel() {
                return outputLevel;
            }

            @Override
            public List<DataFileMeta> files() {
                return files;
            }

            @Override
            public List<Path> externalPaths() {
                return externalPaths;
            }
        };
    }
}
