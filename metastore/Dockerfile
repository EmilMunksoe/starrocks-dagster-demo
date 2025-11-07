FROM apache/hive:standalone-metastore-4.1.0

USER root

RUN microdnf -y update && microdnf -y install gettext postgresql

ENV DB_DRIVER=postgres
# Skip the built in schema migrations - we have our own migration script
ENV SKIP_SCHEMA_INIT=true
ENV SERVICE_NAME=metastore
# Required to load Hadoop Azure modules
ENV HADOOP_OPTIONAL_TOOLS=hadoop-azure
ENV POSTGRES_JDBC_VERSION=42.7.3
RUN curl -L -o ${HIVE_HOME}/lib/postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar

COPY core-site.xml /opt/hive/conf/core-site.xml.tmpl
COPY hive-site.xml /opt/hive/conf/hive-site.xml.tmpl

# New entrypoint
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
