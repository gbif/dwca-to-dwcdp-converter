/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.dwcatodwcdp.converter;

import java.io.File;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DwcaToDwcDpMappingManagerImpl implements DwcaToDwcDpMappingManager {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToDwcDpMappingManagerImpl.class);

  @Override
  public File getMappingFile(String rowType,  File dwcaToDwcDpMappingsDirectory) {
    String mappingFileName = toUnderscoreName(rowType);
    LOG.debug("Mapping file name: {}", mappingFileName);

    File mappingFile = new File(dwcaToDwcDpMappingsDirectory, mappingFileName);
    LOG.debug("Mapping file: {}", mappingFile);

    return mappingFile;
  }

  @Override
  public DwcaToDwcDpTypeMapping getMapping(String rowType, File dwcaToDwcDpMappingsDirectory) {
    File mappingFile = getMappingFile(rowType, dwcaToDwcDpMappingsDirectory);

    if (mappingFile == null || !mappingFile.exists()) {
      LOG.debug("Mapping file not found, skipping deserialization: {}", mappingFile);
      return null;
    }

    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(mappingFile, DwcaToDwcDpTypeMapping.class);
    } catch (Exception e) {
      LOG.error("Error parsing mapping file: {}", mappingFile, e);
      return null;
    }
  }

  private static String toUnderscoreName(String name) {
    Objects.requireNonNull(name);
    return name.replaceAll("[/.:]+", "_") + ".json";
  }
}
