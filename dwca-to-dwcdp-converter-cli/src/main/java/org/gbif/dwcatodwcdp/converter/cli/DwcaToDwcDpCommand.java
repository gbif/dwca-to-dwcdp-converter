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

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.dwcatodwcdp.converter.DwcaToDwcDpConverter;
import org.gbif.dwcatodwcdp.converter.DwcaToDwcDpConverterImpl;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

@MetaInfServices(Command.class)
public class DwcaToDwcDpCommand extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToDwcDpCommand.class);

  private final DwcaToDwcDpConfiguration config;
  private DwcaToDwcDpConverter converter;

  public DwcaToDwcDpCommand() {
    super("dwca-to-dwcdp-converter");
    this.config = new DwcaToDwcDpConfiguration();
  }

  public DwcaToDwcDpCommand(DwcaToDwcDpConfiguration config) {
    super("dwca-to-dwcdp-converter");
    this.config = config;
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected void doRun() {
    LOG.info("DwC-A to DwC DP converter started");

    if (config.dwcaFile == null) {
      LOG.error("You have to provide a DwC archive file to be converted. Exiting.");
      return;
    }

    if (config.dwcDpOutputDir == null) {
      LOG.error("You have to provide an output directory for the DwC DP. Exiting.");
      return;
    }

    converter = new DwcaToDwcDpConverterImpl();

    // TODO: check they are actual files?
    converter.convert(new File(config.dwcaFile), new File(config.dwcDpOutputDir));
  }
}
