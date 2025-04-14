package org.gbif.dwcatodwcdp.converter;

import java.io.File;

public interface DwcaToDwcDpMappingManager {

  File getMappingFile(String rowType, File dwcaToDwcDpMappingsDirectory);

  DwcaToDwcDpTypeMapping getMapping(String rowType, File dwcaToDwcDpMappingsDirectory);
}
