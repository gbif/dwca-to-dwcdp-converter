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
import org.gbif.datapackage.DataPackageTableSchemaRequirement;
import org.gbif.datapackage.RowIterable;
import org.gbif.datapackage.Source;
import org.gbif.datapackage.metadata.DataPackageMetadata;
import org.gbif.utils.file.ClosableReportingIterator;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.frictionlessdata.datapackage.JSONBase;
import io.frictionlessdata.datapackage.Profile;
import io.frictionlessdata.datapackage.resource.FilebasedResource;
import io.frictionlessdata.tableschema.exception.ValidationException;
import io.frictionlessdata.tableschema.schema.Schema;

import lombok.Setter;

public class DataPackageGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(DataPackageGenerator.class);

  private enum STATE {
    WAITING, STARTED, DATARESOURCES, METADATA, BUNDLING, COMPLETED, ARCHIVING, VALIDATING, CANCELLED, FAILED
  }

  private static final Pattern ESCAPE_CHARS = Pattern.compile("[\t\n\r]");
  private static final Random RANDOM = new Random();
  private static final String DWC_DP_IDENTIFIER = "http://rs.gbif.org/data-packages/dwc-dp";

  private ObjectMapper objectMapper = new ObjectMapper();
  private STATE state = STATE.WAITING;
  private Exception exception;
  private File dataPackageFolder;
  private io.frictionlessdata.datapackage.Package dataPackage;
  private String dataPackageFileName;
  private int currRecords = 0;
  private int currRecordsSkipped = 0;
  @Setter
  private String currSchemaName;
  @Setter
  private DataPackageSchema dataPackageSchema;
  private String currTableSchemaName;
  // record counts by extension <rowType, count>
  private Map<String, Integer> recordsByTableSchema = new HashMap<>();

  @Setter
  private Map<String, DataPackageMapping> dataPackageMappings = new HashMap<>();
  @Setter
  private File dwcDpOutputDirectory;
  @Setter
  private File eml;
  private DataPackageMetadata dataPackageMetadata;

  // TODO: make sure it is used
  private File tempArchive;

  // TODO: what parameters to take?
  public DataPackageGenerator() {
  }

  public Map<String, Integer> generate() throws Exception {
    try {
      setState(STATE.STARTED);

      // initial reporting
      LOG.info("Data Package generation started");

      // create a temp dir to copy all files to
      // TODO: make sure properly created
      dataPackageFolder = tmpDir();

      // copy datapackage descriptor file (datapackage.json)
      addMetadata();
      // create data resources
      createDataResources();

      // validation is a part of frictionless datapackage generation
      // zip archive and copy to resource folder
      bundleArchive();

      writeMappings();

      // reporting
      LOG.info("Archive generated successfully");

      // set final state
      setState(STATE.COMPLETED);

      return recordsByTableSchema;
    } catch (GeneratorException e) {
      // set the last error report!
      setState(e);

      // write exception to publication log file when IPT is in debug mode, otherwise just log it
      LOG.error("Exception occurred trying to generate data package: {}", e.getMessage(), e);

      // rethrow exception, which gets wrapped in an ExecutionException and re caught when calling Future.get
      throw e;
    } catch (InterruptedException e) {
      setState(e);
      LOG.error("Exception occurred trying to generate data package: {}", e.getMessage(), e);
      throw e;
    } catch (Exception e) {
      setState(e);
      LOG.error("Exception occurred trying to generate data package: {}", e.getMessage(), e);
      throw new GeneratorException(e);
    } finally {
      // cleanup temp dir that was used to store data package files
      if (dataPackageFolder != null && dataPackageFolder.exists()) {
        LOG.info("Cleaning up output directory {}",  dataPackageFolder.getAbsolutePath());
        FileUtils.deleteQuietly(dataPackageFolder);
      }
    }
  }

  protected boolean completed() {
    return STATE.COMPLETED == this.state;
  }

  protected Exception currentException() {
    return exception;
  }

  protected String currentState() {
    switch (state) {
      case WAITING:
        return "Not started yet";
      case STARTED:
        return "Starting Data Package generation";
      case DATARESOURCES:
        return "Processing record " + currRecords + " for Data Resource <em>" + currTableSchemaName + "</em>";
      case METADATA:
        return "Creating metadata files";
      case BUNDLING:
        return "Compressing Data Package (archive)";
      case COMPLETED:
        return "Data Package generated!";
      case VALIDATING:
        return "Validating Data Package, " + currRecords + " for Data Resource <em>" + currTableSchemaName + "</em>";
      case ARCHIVING:
        return "Archiving version of data package";
      case CANCELLED:
        return "Data Package generation cancelled";
      case FAILED:
        return "Data Package generation failed";
      default:
        return "You should never see this";
    }
  }

  /**
   * Zips the data package folder. A temp version is created first, and when successful, it's moved into the resource's
   * data directory.
   *
   * @throws GeneratorException if data package could not be zipped or moved
   * @throws InterruptedException if executing thread was interrupted
   */
  private void bundleArchive() throws Exception {
    setState(STATE.BUNDLING);
    File zip;
    try {
      // create zip
      zip = tmpFile("datapackage", ".zip");
      dataPackageFileName = zip.getName();

      dataPackage.write(zip, this::writeEMLMetadata, true);

      if (!zip.exists()) {
        throw new GeneratorException("Archive bundling failed: temp archive not created: " + zip.getAbsolutePath());
      }
    } catch (IOException e) {
      throw new GeneratorException("Problem occurred while bundling data package", e);
    } finally {
      // TODO: cleanup directories if needed
    }
    // final reporting
    LOG.info("Archive has been compressed");
  }

  private void writeMappings() throws IOException {
    File mappingsDirectory = new File(dwcDpOutputDirectory, "mappings-" + dataPackageFileName.replace(".zip", ""));
    mappingsDirectory.mkdirs();

    for (Map.Entry<String, DataPackageMapping> entry : dataPackageMappings.entrySet()) {
      String key = entry.getKey();
      DataPackageMapping value = entry.getValue();

      // Make filename safe if needed (e.g., remove special characters)
      String safeFileName = key.replaceAll("[^a-zA-Z0-9\\-_.]", "_");

      File file = new File(mappingsDirectory, safeFileName + ".json");
      objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
      objectMapper.writeValue(file, value.getFields());
    }
  }

  /**
   * Apart from a standard frictionless metadata, DwCA v2 occurrence must contain a EML file.
   */
  private void writeEMLMetadata(Path outputDir) {
    Path target = outputDir.getFileSystem().getPath("eml.xml");
    try {
      Path sourcePath = eml.toPath();
      Files.copy(sourcePath, target, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      LOG.error("Failed to write eml.xml", e);
    }
  }

  /**
   * Sets only the state of the worker. The final StatusReport is generated at the end.
   *
   * @param s STATE of worker
   */
  private void setState(STATE s) {
    state = s;
  }

  /**
   * Sets an exception and state of the worker to FAILED. The final StatusReport is generated at the end.
   *
   * @param e exception
   */
  private void setState(Exception e) {
    exception = e;
    state = (exception instanceof InterruptedException) ? STATE.CANCELLED : STATE.FAILED;
  }

  /**
   * Create data files.
   *
   * @throws GeneratorException if the resource had no core file that was mapped
   * @throws InterruptedException if the thread was interrupted
   */
  private void createDataResources() throws GeneratorException, InterruptedException {
    setState(STATE.DATARESOURCES);
    if (dataPackageMappings.isEmpty()) {
      throw new GeneratorException("Data package identifier or mappings are not set");
    }

    Set<String> mappedTableSchemas = dataPackageMappings.values().stream()
        .map(DataPackageMapping::getDataPackageTableSchemaName)
        .map(DataPackageTableSchemaName::getName)
        .collect(Collectors.toSet());

    // before starting to add a table schema, check all required schemas mapped
    checkRequiredTableSchemasMapped(mappedTableSchemas, dataPackageSchema);

    for (DataPackageTableSchema tableSchema : dataPackageSchema.getTableSchemas()) {
      // skip un-mapped (optional) schemas
      if (!mappedTableSchemas.contains(tableSchema.getName())) {
        continue;
      }

      try {
        addDataResource(tableSchema);
      } catch (IOException | IllegalArgumentException e) {
        throw new GeneratorException("Problem occurred while writing Data Resource", e);
      }
    }

    // final reporting
    LOG.info("All Data Resources completed");
  }

  /**
   * Checks if all required schemas mapped, otherwise throws an exception.
   *
   * @param mappedTableSchemas mapped table schemas
   * @param dataPackageSchema data schema
   */
  private void checkRequiredTableSchemasMapped(Set<String> mappedTableSchemas, DataPackageSchema dataPackageSchema)
      throws GeneratorException {
    DataPackageTableSchemaRequirement requirements = dataPackageSchema.getTableSchemasRequirements();

    if (requirements != null) {
      DataPackageTableSchemaRequirement.ValidationResult validationResult = requirements.validate(mappedTableSchemas);

      if (!validationResult.isValid()) {
        throw new GeneratorException(validationResult.getReason());
      }
    }
  }

  /**
   * Adds a single data resource for a tableSchema mapping.
   *
   * @throws IllegalArgumentException if not all mappings are mapped to the same extension
   * @throws InterruptedException if the thread was interrupted
   * @throws IOException if problems occurred while persisting new data resources
   * @throws GeneratorException if any problem was encountered writing data resources
   */
  public void addDataResource(DataPackageTableSchema tableSchema) throws IOException,
      IllegalArgumentException, InterruptedException, GeneratorException {
    if (tableSchema == null) {
      LOG.warn("Failed to add a data resource - table schema is not present");
      return;
    }

    if (dataPackageMappings.isEmpty()) {
      LOG.warn("Failed to add a data resource - data mappings are empty");
      return;
    }

    LOG.debug("Adding data resource {}", tableSchema.getName());

    // update reporting
    currRecords = 0;
    currRecordsSkipped = 0;
    currTableSchemaName = tableSchema.getName();

    List<DataPackageField> fields = tableSchema.getFields();
    List<DataPackageField> mappedDataPackageFields = getOrderedMappedDataPackageFields(tableSchema.getName(), fields);

    // file header
    String header = mappedDataPackageFields.stream()
        .map(DataPackageField::getName)
        .collect(Collectors.joining(",", "", "\n"));

    // total column count
    int totalColumns = mappedDataPackageFields.size();

    String fn = tableSchema.getName() + ".csv";
    File dataFile = new File(dataPackageFolder, fn);
    //noinspection ResultOfMethodCallIgnored
    dataFile.getParentFile().mkdirs();
    //noinspection ResultOfMethodCallIgnored
    dataFile.createNewFile();

    if (!dataFile.exists()) {
      LOG.error("Fatal Package Generator Error encountered while trying to create the data file {}", dataFile.getName());
      throw new GeneratorException("Error creating the data file " + dataFile.getName());
    }

    // ready to go through each mapping and dump the data
    try (Writer writer = org.gbif.utils.file.FileUtils.startNewUtf8File(dataFile)) {
      LOG.info("Start creating Data Resource {}", tableSchema.getName());

      DataPackageMapping dataPackageMapping = dataPackageMappings.get(tableSchema.getName());

      // write header line 1 time only to file
      writer.write(header);
      dumpData(writer, dataPackageMapping, dataPackageMapping.getFields(), totalColumns);

      // store record number by extension rowType
      recordsByTableSchema.put(tableSchema.getName(), currRecords);
    } catch (IOException e) {
      // some error writing this file, report
      LOG.error("Fatal Package Generator Error encountered while writing header line to Data Resource {}", tableSchema.getName(), e);
      // set last error report!
      setState(e);
      throw new GeneratorException("Error writing header line to Data Resource " + tableSchema.getName(), e);
    }

    // create resource from file
    @SuppressWarnings({"rawtypes", "unchecked"})
    io.frictionlessdata.datapackage.resource.Resource packageResource =
        new FilebasedResource(
            tableSchema.getName(),
            Collections.singleton(new File(fn)),
            dataPackageFolder);
    packageResource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
    packageResource.setFormat(io.frictionlessdata.datapackage.resource.Resource.FORMAT_CSV);
    if (tableSchema.getUrl() != null) {
      ((JSONBase) packageResource).getOriginalReferences().put(JSONBase.JSON_KEY_SCHEMA, tableSchema.getUrl().toString());
    }

    try {
      Schema schema = Schema.fromJson(tableSchema.getUrl(), true);
      packageResource.setSchema(schema);
    } catch (ValidationException e) {
      LOG.error("Failed to validate schema {}. Errors: {}", tableSchema.getName(), e.getMessages(), e);
      // set the last error report!
      setState(e);
      throw new GeneratorException("Validation error while adding schema file", e);
    } catch (Exception e) {
      LOG.error("Fatal Package Generator Error encountered while adding schema data {}", tableSchema.getIdentifier(), e);
      // set the last error report!
      setState(e);
      throw new GeneratorException("Error while adding schema file", e);
    }

    // add resource to package
    if (dataPackage == null) {
      dataPackage = new io.frictionlessdata.datapackage.Package(Collections.singleton(packageResource));
    } else {
      dataPackage.addResource(packageResource);
    }

    // final reporting
    LOG.info("Data Resource {} created with {} records and {} columns", currTableSchemaName, currRecords, totalColumns);
    // how many records were skipped?
    if (currRecordsSkipped > 0) {
      LOG.warn("!!! {} records were skipped for {} due to errors interpreting line, or because the line was empty", currRecordsSkipped, currTableSchemaName);
    }
  }

  private List<DataPackageField> getOrderedMappedDataPackageFields(String tableSchemaName, List<DataPackageField> tableSchemaFields) {
    DataPackageMapping dpm = dataPackageMappings.get(tableSchemaName);
    // mapped
    List<DataPackageFieldMapping> fieldMappings = dpm.getFields();

    return fieldMappings.stream()
        .map(DataPackageFieldMapping::getField)
        .collect(Collectors.toList());
  }

  /**
   * Write data resource for mappings.
   *
   * @param writer file writer for single data resource
   * @param schemaMapping schema mapping
   * @param tableSchemaFieldMappings field mappings
   * @param dataFileRowSize number of columns in data resource
   * @throws GeneratorException if there was an error writing data resource for mapping.
   * @throws InterruptedException if the thread was interrupted
   */
  private void dumpData(Writer writer, DataPackageMapping schemaMapping,
                        List<DataPackageFieldMapping> tableSchemaFieldMappings, int dataFileRowSize)
      throws GeneratorException, InterruptedException {
    int recordsWithError = 0;
    int linesWithWrongColumnNumber = 0;
    int emptyLines = 0;
    ClosableReportingIterator<String[]> iter = null;
    int line = 0;
    Optional<Integer> maxMappedColumnIndexOpt = tableSchemaFieldMappings.stream()
        .map(DataPackageFieldMapping::getIndex)
        .filter(Objects::nonNull)
        .max(Comparator.naturalOrder());

    try {
      // get the source iterator
      iter = rowIterator(schemaMapping.getSource());
      // TODO: make sure ignore header row is considered - for now just skip the first row
      if (iter.hasNext()) {
        iter.next();
      }

      while (iter.hasNext()) {
        line++;
        String[] in = iter.next();
        if (in == null || in.length == 0) {
          continue;
        }

        // Exception on reading row was encountered, meaning record is incomplete and not written
        if (iter.hasRowError()) {
          LOG.warn("Error reading line #{}\n{}", line, iter.getErrorMessage());
          recordsWithError++;
          currRecordsSkipped++;
        }
        // empty line was encountered, meaning record only contains empty values and not written
        else if (isEmptyLine(in)) {
          LOG.warn("Empty line was skipped. SourceBase:{} Line #{}: {}", schemaMapping.getSource().getName(), line, printLine(in));
          emptyLines++;
          currRecordsSkipped++;
        } else {

          if (maxMappedColumnIndexOpt.isPresent() && in.length <= maxMappedColumnIndexOpt.get()) {
            LOG.warn("Line with fewer columns than mapped. SourceBase:{} Line #{} has {} Columns: {}", schemaMapping.getSource().getName(), line, in.length, printLine(in));
            // input row is smaller than the highest mapped column. Resize array by adding nulls
            String[] in2 = new String[maxMappedColumnIndexOpt.get() + 1];
            System.arraycopy(in, 0, in2, 0, in.length);
            in = in2;
            linesWithWrongColumnNumber++;
          }

           // TODO: still apply translations somehow? - but there are no translations - rename
           // initialize translated values and add id column
           String[] translated = new String[dataFileRowSize];

           // filter this record?
           applyTranslations(tableSchemaFieldMappings, in, translated);

          // concatenate values
          String newRow = commaRow(translated);

          // write a new row (skip if null)
          if (newRow != null) {
            writer.write(newRow);
            currRecords++;
          }
        }
      }
    } catch (InterruptedException e) {
      // set last error report!
      setState(e);
      throw e;
    } catch (Exception e) {
      // some error writing this file, report
      LOG.error("Fatal Data Package Generator Error encountered", e);
      // set last error report!
      setState(e);
      throw new GeneratorException("Error writing Data Resource for mapping " + currTableSchemaName
          + " in source " + schemaMapping.getSource().getName() + ", line " + line, e);
    } finally {
      if (iter != null) {
        // Exception on advancing cursor encountered?
        if (!iter.hasRowError() && iter.getErrorMessage() != null) {
          LOG.error("Error reading data: {}", iter.getErrorMessage());
        }
        try {
          iter.close();
        } catch (Exception e) {
          LOG.error("Error while closing iterator", e);
        }
      }
    }

    // common message part used in constructing all reporting messages below
    String mp = " for mapping " + schemaMapping.getDataPackageSchema().getTitle() + " in source " + schemaMapping.getSource().getName();

    // add lines incomplete message
    if (recordsWithError > 0) {
      LOG.warn("{} record(s) skipped due to errors{}", recordsWithError, mp);
    } else {
      LOG.info("No lines were skipped due to errors{}", mp);
    }

    // add empty lines message
    if (emptyLines > 0) {
      LOG.warn("{} empty line(s) skipped{}", emptyLines, mp);
    } else {
      LOG.info("No lines were skipped due to errors{}", mp);
    }

    // add wrong lines user message
    if (linesWithWrongColumnNumber > 0) {
      LOG.warn("{} line(s) with fewer columns than mapped{}", linesWithWrongColumnNumber, mp);
    } else {
      LOG.info("No lines with fewer columns than mapped{}", mp);
    }
  }

  /**
   * Apply translations or default values to row, for all mapped properties.
   * </br>
   * The method starts by iterating through all mapped properties, checking each one if it has been translated or a
   * default value provided. The original value in the row is then replaced with the translated or default value.
   * A record array representing the values to be written to the data resource is also updated.
   *
   * @param inCols values array, of columns in row that have been mapped
   * @param in values array, of all columns in row
   * @param translated translated values
   */
  private void applyTranslations(List<DataPackageFieldMapping> inCols, String[] in, String[] translated) {
    for (int i = 0; i < inCols.size(); i++) {
      DataPackageFieldMapping mapping = inCols.get(i);
      String val = null;
      if (mapping != null) {
        if (mapping.getIndex() != null) {
          val = in[mapping.getIndex()];
          // TODO: no translation needed - refactor when everything's done
        }
        // use default value for null values
        if (val == null) {
          val = mapping.getDefaultValue();
        }
      }
      // add value to data resource record
      translated[i] = val;
    }
  }

  /**
   * Generates a single comma delimited row from the list of values of the provided array.
   * </br>
   * Note all line breaking characters in the value get replaced with an empty string before its added to the row.
   * </br>
   * The row ends in a newline character.
   *
   * @param columns the array of values from the source
   *
   * @return the comma delimited String, {@code null} if provided array only contained null values
   */
  protected String commaRow(String[] columns) {
    Objects.requireNonNull(columns);
    boolean empty = true;

    for (int i = 0; i < columns.length; i++) {
      if (columns[i] != null) {
        empty = false;
        columns[i] = StringUtils.trimToNull(ESCAPE_CHARS.matcher(columns[i]).replaceAll(""));

        boolean containsDoubleQuotes = StringUtils.contains(columns[i], '"');
        boolean containsComma = StringUtils.contains(columns[i], ',');

        // Escape double quotes if present
        if (containsDoubleQuotes) {
          // escape double quotes
          columns[i] = StringUtils.replace(columns[i], "\"", "\"\"");
        }

        // commas break the whole line, wrap in double quotes
        // same for double quotes
        if (containsComma || containsDoubleQuotes) {
          columns[i] = StringUtils.wrap(columns[i], '"');
        }
      }
    }

    if (empty) {
      return null;
    }

    return StringUtils.join(columns, ',') + "\n";
  }

  /**
   * Check if each string in array is empty. Method joins each string together and then checks if it is blank. A
   * blank string represents an empty line in a source data resource.
   *
   * @param line string array
   *
   * @return true if each string in array is empty, false otherwise
   */
  private boolean isEmptyLine(String[] line) {
    String joined = Arrays.stream(line)
        .filter(Objects::nonNull)
        .collect(Collectors.joining(""));
    return StringUtils.isBlank(joined);
  }

  /**
   * Print a line representation of a string array used for logging.
   *
   * @param in String array
   * @return line
   */
  private String printLine(String[] in) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < in.length; i++) {
      sb.append(in[i]);
      if (i != in.length - 1) {
        sb.append("; ");
      }
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Adds metadata to data package.
   *
   * @throws GeneratorException if there are issues with metadata file
   * @throws InterruptedException if executing thread was interrupted
   */
  private void addMetadata() throws GeneratorException, InterruptedException {
    setState(DataPackageGenerator.STATE.METADATA);
    try {
      String type = "dwc-dp";
      // TODO: make sure metadata is added (both EML and datapackage metadata)

    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new GeneratorException("Problem occurred while adding metadata file to data package folder", e);
    }
    // final reporting
    LOG.info("Metadata added");
  }

  private void setDataPackageProperty(String name, Object property) {
    if (property != null) {
      dataPackage.setProperty(name, property);
    }
  }

  private void setDataPackageStringProperty(String name, String property) {
    if (StringUtils.isNotEmpty(property)) {
      dataPackage.setProperty(name, property);
    }
  }

  @SuppressWarnings("rawtypes")
  private void setDataPackageCollectionProperty(String name, Collection property) {
    if (property != null && !property.isEmpty()) {
      dataPackage.setProperty(name, property);
    }
  }

  public File tmpDir() {
    String random = String.valueOf(RANDOM.nextLong());
    return new File(dwcDpOutputDirectory, "dir" + random);
  }

  public File tmpFile(String path) {
    // TODO: make sure data dir is properly set
    return new File(dwcDpOutputDirectory, path);
  }

  public File tmpFile(String prefix, String suffix) {
    String random = String.valueOf(RANDOM.nextInt());
    return tmpFile(prefix + random + suffix);
  }

  public ClosableReportingIterator<String[]> rowIterator(Source source) throws Exception {
    if (source == null) {
      return null;
    }

    try {
      return ((RowIterable) source).rowIterator();
    } catch (Exception e) {
      LOG.error("Exception while reading source {}", source.getName(), e);
      throw new Exception("Can't build iterator for source " + source.getName() + " :" + e.getMessage());
    }
  }

}
