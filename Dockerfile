# python image with uv preinstalled
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

# install OpenJDK
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget git && \
    apt-get clean

# set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

# Copy the project into the image
ADD . /app

# Sync the project into a new environment, using the frozen lockfile
WORKDIR /app
RUN uv sync --frozen

# Download Neo4j JDBC driver
RUN mkdir -p drivers && \
    wget -O drivers/liquibase-neo4j-4.31.1-full.jar https://github.com/liquibase/liquibase-neo4j/releases/download/v4.31.1/liquibase-neo4j-4.31.1-full.jar

# Startup command to keep the container running until explicitly stopped
CMD ["sh", "-c", "echo 'Container started' && tail -f /dev/null"]