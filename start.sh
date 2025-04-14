#mvn clean package

java -agentlib:jdwp="transport=dt_socket,server=y,suspend=n,address=*:5005" \
      -jar dwca-to-dwcdp-converter-cli/target/dwca-to-dwcdp-converter-cli.jar dwca-to-dwcdp-converter \
      --log-config dwca-to-dwcdp-converter-cli/src/main/resources/logback.xml \
      --dwca /Users/rvl320/IdeaProjects/dwca-to-dwcdp-converter/input-dwca/dwca-aves-tanzanian-v1.9.zip \
      --dwcdp /Users/rvl320/IdeaProjects/dwca-to-dwcdp-converter/output-dwcdp \
      --mappings /Users/rvl320/IdeaProjects/dwca-to-dwcdp-converter/dwca-to-dwcdp-converter-common/src/main/resources/dwca-to-dwcdp-mappings