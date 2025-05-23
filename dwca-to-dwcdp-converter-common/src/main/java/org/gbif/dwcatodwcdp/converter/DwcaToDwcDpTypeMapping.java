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

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnySetter;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class DwcaToDwcDpTypeMapping {

  private final Map<String, DwcaToDwcDpTermMapping> mappings = new HashMap<>();

  @JsonAnySetter
  public void addMapping(String key, DwcaToDwcDpTermMapping value) {
    mappings.put(key, value);
  }

  public DwcaToDwcDpTermMapping getMapping(String key) {
    return mappings.get(key);
  }
}
