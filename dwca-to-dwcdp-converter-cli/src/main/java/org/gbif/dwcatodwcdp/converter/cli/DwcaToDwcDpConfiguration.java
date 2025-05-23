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
package org.gbif.dwcatodwcdp.converter.cli;

import com.beust.jcommander.Parameter;

import java.util.StringJoiner;

public class DwcaToDwcDpConfiguration {

  @Parameter(names = {"--dwca-file", "--dwca"})
  public String dwcaFile;

  @Parameter(names = {"--dwc-dp-output-dir", "--dwcdp"})
  public String dwcDpOutputDir;

  @Parameter(names = {"--dwca-to-dwc-dp-mappings", "--mappings"})
  public String dwcaToDwcDpMappings;

  @Override
  public String toString() {
    return new StringJoiner(", ", DwcaToDwcDpConfiguration.class.getSimpleName() + "[", "]")
        .add("dwcaFile='" + dwcaFile + "'")
        .add("dwcDpOutputDir='" + dwcDpOutputDir + "'")
        .add("dwcaToDwcDpMappings='" + dwcaToDwcDpMappings + "'")
        .toString();
  }
}
