java -agentlib:jdwp="transport=dt_socket,server=y,suspend=n,address=*:5005" \
      -jar dwca-to-dwcdp-converter-cli/target/dwca-to-dwcdp-converter-cli.jar dwca-to-dwcdp-converter \
      --log-config dwca-to-dwcdp-converter-cli/src/main/resources/logback.xml \
      --dwca ... \
      --dwcdp ... \
      --mappings dwca-to-dwcdp-converter-common/src/main/resources/dwca-to-dwcdp-mappings