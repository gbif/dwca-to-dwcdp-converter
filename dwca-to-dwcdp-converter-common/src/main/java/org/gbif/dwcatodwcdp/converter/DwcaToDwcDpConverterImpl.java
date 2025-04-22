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

import org.gbif.datapackage.DataPackageField;
import org.gbif.datapackage.DataPackageFieldMapping;
import org.gbif.datapackage.DataPackageMapping;
import org.gbif.datapackage.DataPackageSchema;
import org.gbif.datapackage.DataPackageTableSchema;
import org.gbif.datapackage.DataPackageTableSchemaName;
import org.gbif.datapackage.TextFileSource;
import org.gbif.dwc.Archive;
import org.gbif.dwc.ArchiveField;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.CompressionUtil;
import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DwcaToDwcDpConverterImpl implements DwcaToDwcDpConverter {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToDwcDpConverterImpl.class);

  private final ObjectMapper objectMapper;
  private final DwcaToDwcDpMappingManager mappingManager;
  private DataPackageSchema dataPackageSchema;
  private final Map<String, DataPackageTableSchema> tableSchemas = new HashMap<>();
  private final Map<String, DataPackageMapping> dataMappings = new HashMap<>();
  private DataPackageGenerator packageGenerator;

  public DwcaToDwcDpConverterImpl() {
    this.objectMapper = provideObjectMapper();
    this.dataPackageSchema = provideDataPackageSchema();
    this.mappingManager = new DwcaToDwcDpMappingManagerImpl();
    this.packageGenerator = new DataPackageGenerator();
  }

  // TODO: 31/03/2025 use parameters instead of list of input variables?
  // TODO: declare any Exception thrown?
  @Override
  public void convert(File dwcaFile, File dwcDpOutputDirectory, File dwcaToDwcDpMappingsDirectory) {
    LOG.info("Converting the DwC-A to the DWC DP");

    Archive dwca;
    File tmpDirWithUnpackedDwca = null;

    try {
      tmpDirWithUnpackedDwca = FileUtils.createTempDir();
      LOG.debug("Temp dir created: {}", tmpDirWithUnpackedDwca.getAbsolutePath());
      CompressionUtil.decompressFile(tmpDirWithUnpackedDwca, dwcaFile, true);
    } catch (IOException e) {
      LOG.error("Failed to unpack DwC-A", e);
      cleanUpDirectory(tmpDirWithUnpackedDwca);
      return;
    }

    try {
      dwca = DwcFiles.fromLocation(tmpDirWithUnpackedDwca.toPath());
    } catch (IOException e) {
      LOG.error("Failed to create Archive class from the provided DwC-A", e);
      cleanUpDirectory(tmpDirWithUnpackedDwca);
      return;
    }

    ArchiveFile core = dwca.getCore();
    File eml = dwca.getMetadataLocationFile();
    Set<ArchiveFile> extensions = dwca.getExtensions();

    LOG.debug("Core file: {}", core);

    // process core and extension DwC-A files and produce data package mappings
    processCoreFile(core, dwcaToDwcDpMappingsDirectory);
    processExtensionFiles(extensions, dwcaToDwcDpMappingsDirectory);
    postProcessDataMappings();

    packageGenerator.setCurrSchemaName("dwc-dp");
    packageGenerator.setDataPackageSchema(dataPackageSchema);
    packageGenerator.setDataPackageMappings(dataMappings);
    packageGenerator.setDwcDpOutputDirectory(dwcDpOutputDirectory);
    packageGenerator.setEml(eml);

    // generate data package
    try {
      packageGenerator.generate();
    } catch (Exception e) {
      LOG.error("Failed to generate DwC-A with DataPackageGenerator", e);
    }

    cleanUpDirectory(tmpDirWithUnpackedDwca);
  }

  private static void cleanUpDirectory(File tmpDirWithUnpackedDwca) {
    if (tmpDirWithUnpackedDwca != null && tmpDirWithUnpackedDwca.exists()) {
      LOG.debug("Cleaning up temporary directory: {}", tmpDirWithUnpackedDwca.getAbsolutePath());
      FileUtils.deleteDirectoryRecursively(tmpDirWithUnpackedDwca);
    }
  }

  private void processCoreFile(ArchiveFile core, File dwcaToDwcDpMappingsDirectory) {
    LOG.debug("Processing the DwC-A core file");
    // TODO: null check etc.
    Term rowType = core.getRowType();
    String rowTypeName = rowType.toString();
    LOG.debug("Row type: {}", rowTypeName);

    DwcaToDwcDpTypeMapping dwcaToDwcDpMapping = mappingManager.getMapping(rowTypeName, dwcaToDwcDpMappingsDirectory);

    if (dwcaToDwcDpMapping != null) {
      LOG.debug("Successfully deserialized the mapping file for the row type [{}]", rowTypeName);
    } else {
      LOG.debug("Skipping core file processing, mapping wasn't deserialized for the row type [{}]", rowTypeName);
      return;
    }

    Set<Term> terms = core.getTerms();
    LOG.debug("Core terms found: {}", terms);

    Map<String, DataPackageMapping> dataPackageMappingsByResourceName = new HashMap<>();

    for (Term term : terms) {
      String termPrefixedName = term.prefixedName();
      DwcaToDwcDpTermMapping termMapping = dwcaToDwcDpMapping.getMapping(termPrefixedName);

      if (termMapping == null) {
        LOG.warn("DwC-A to DwC dp term mapping wasn't found for the term [{}]", termPrefixedName);
      } else {
        LOG.debug("Successfully found DwC-A to DwC DP term mapping for the term [{}]. Mapping: {}", term, termMapping);

        ArchiveField field = core.getField(term);
        LOG.debug("Field [{}]", field);

        String tableSchemaName = termMapping.getResource();
        String fieldName = termMapping.getField();

        DataPackageTableSchema tableSchema = getTableSchema(tableSchemaName);
        DataPackageField dataPackageField = getField(fieldName, tableSchema);

        DataPackageFieldMapping dpFieldMapping = DataPackageFieldMapping.builder()
            .index(field.getIndex())
            .field(dataPackageField)
            .build();

        // get existing DP mapping. Otherwise, initialize a new one and store it
        DataPackageMapping dataPackageMapping = dataPackageMappingsByResourceName.get(tableSchemaName);
        if (dataPackageMapping == null) {
          dataPackageMapping = initializeNewDataPackageMapping(core, tableSchemaName);
          dataPackageMappingsByResourceName.put(tableSchemaName, dataPackageMapping);
        }

        // add new field mapping
        dataPackageMapping.getFields().add(dpFieldMapping);

        // store mapping
        dataMappings.putIfAbsent(tableSchemaName, dataPackageMapping);
      }
    }
  }

  private void postProcessDataMappings() {
    postProcessEventMapping();
  }

  private void postProcessEventMapping() {
    DataPackageMapping eventMapping = dataMappings.get("event");

    if (!mappingContainsField(eventMapping, "eventID")) {
      DataPackageFieldMapping dpfm = new DataPackageFieldMapping();

      // TODO: extract properly
      DataPackageTableSchema eventTableSchema = tableSchemas.get("event");
      DataPackageField eventIdField = eventTableSchema.getFields().stream()
          .filter(p -> "eventID".equals(p.getName()))
          .findFirst()
          .get();

      dpfm.setField(eventIdField);
      dpfm.setDefaultValue(UUID.randomUUID().toString());

      eventMapping.getFields().add(dpfm);
    }
  }

  private boolean mappingContainsField(DataPackageMapping mapping, String fieldName) {
    return mapping.getFields().stream()
        .map(DataPackageFieldMapping::getField)
        .map(DataPackageField::getName)
        .anyMatch(fn -> fn.equals(fieldName));
  }

  private void processExtensionFiles(Set<ArchiveFile> extensions, File dwcaToDwcDpMappingsDirectory) {
    LOG.debug("Processing the DwC-A extension files");
    LOG.debug("Extensions processing not implemented yet!");
  }

  private DataPackageField getField(String fieldName, DataPackageTableSchema tableSchema) {
    return tableSchema.getFields().stream()
        .filter(f -> f.getName().equals(fieldName))
        .findFirst()
        .orElse(null);
  }

  private DataPackageMapping initializeNewDataPackageMapping(ArchiveFile core, String tableSchemaName) {
    DataPackageMapping dataPackageMapping;
    dataPackageMapping = new DataPackageMapping();
    dataPackageMapping.setDataPackageSchema(dataPackageSchema);
    dataPackageMapping.setFieldsMapped(dataPackageMapping.getFieldsMapped() + 1);
    dataPackageMapping.setDataPackageTableSchemaName(new DataPackageTableSchemaName(tableSchemaName));
    dataPackageMapping.setLastModified(new Date());

    TextFileSource src = new TextFileSource();
    File coreDataFile = core.getFirstLocationFile();
    // TODO: hardcode "occurrence" - for other core types and extensions will be different
    src.setName("occurrence");
    src.setFile(coreDataFile);
    dataPackageMapping.setSource(src);

    return dataPackageMapping;
  }

  private DataPackageTableSchema getTableSchema(String tableSchemaName) {
    // short version, no fields
    DataPackageTableSchema tableSchema = tableSchemas.get(tableSchemaName);

    if (tableSchema == null) {
      URL tableSchemaUrl = dataPackageSchema.getTableSchemas().stream()
          .filter(p -> p.getName().equals(tableSchemaName))
          .map(DataPackageTableSchema::getUrl)
          .findFirst()
          .orElse(null);

      if (tableSchemaUrl != null) {
        try {
          tableSchema = objectMapper.readValue(tableSchemaUrl, DataPackageTableSchema.class);
          tableSchemas.put(tableSchemaName, tableSchema);
        } catch (IOException e) {
          LOG.error("Failed to read JSON from URL [{}]", tableSchemaUrl, e);
        }
      } else {
        LOG.error("Can't download data package table schema [{}] - URL is null", tableSchemaName);
      }
    }

    return tableSchema;
  }

  private DataPackageSchema provideDataPackageSchema() {
    DataPackageSchema dps;

    try {
      // TODO: URL should be configurable
      dps = objectMapper.readValue(URI.create("https://gbrds.gbif-uat.org/registry/dataPackages/dwc-dp/0.1").toURL(), DataPackageSchema.class);

      LOG.debug("DataPackage schema downloaded from GBRDS: {}", dps.getUrl());
      LOG.debug("DataPackage schema table schemas: {}", dps.getTableSchemas().stream().map(DataPackageTableSchema::getName).collect(Collectors.joining()));
    } catch (Exception e) {
      LOG.error("Failed to download data package schema", e);
      return null;
    }

    Set<DataPackageTableSchema> tableSchemasWithFields = new HashSet<>();
    URL tableSchemaUrl = null;
    try {
      for (DataPackageTableSchema dts : dps.getTableSchemas()) {
        tableSchemaUrl = dts.getUrl();
        DataPackageTableSchema dataPackageTableSchema = objectMapper.readValue(tableSchemaUrl, DataPackageTableSchema.class);
        tableSchemasWithFields.add(dataPackageTableSchema);
      }
    } catch (Exception e) {
      LOG.error("Failed to download data package table schema {}", tableSchemaUrl, e);
      return null;
    }

    dps.setTableSchemas(tableSchemasWithFields);

    return dps;
  }

  public ObjectMapper provideObjectMapper() {
    ObjectMapper om = new ObjectMapper();
    om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    return om;
  }
}
