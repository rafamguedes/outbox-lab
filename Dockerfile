# syntax=docker/dockerfile:1
FROM maven:3.9.11-eclipse-temurin-21-alpine AS build
WORKDIR /app
ENV MAVEN_OPTS="-Xms32m -Xmx192m -XX:MaxMetaspaceSize=128m -XX:+UseSerialGC -XX:ActiveProcessorCount=1"
# Camada independente do código: mudanças na aplicação reaproveitam o runtime.
RUN jlink -J-Xmx192m -J-XX:+UseSerialGC -J-XX:ActiveProcessorCount=1 \
    --add-modules java.base,java.desktop,java.instrument,java.management,java.naming,java.net.http,java.security.jgss,java.sql,jdk.crypto.ec,jdk.unsupported,jdk.charsets \
    --strip-debug --no-header-files --no-man-pages --compress=2 \
    --output /opt/java-minimal
COPY pom.xml .
COPY src src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -Dmaven.test.skip=true package

FROM alpine:3.23
RUN apk add --no-cache libstdc++ zlib ca-certificates \
    && addgroup -S app && adduser -S app -G app
ENV JAVA_HOME=/opt/java
ENV PATH="/opt/java/bin:${PATH}"
# Heap não é a memória total da JVM. Deixamos espaço para classes, threads e memória nativa.
ENV JAVA_TOOL_OPTIONS="-Xms16m -Xmx64m -XX:MaxMetaspaceSize=80m -XX:ReservedCodeCacheSize=16m -XX:MaxDirectMemorySize=8m -Xss384k -XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:ActiveProcessorCount=1 -XX:+ExitOnOutOfMemoryError"
WORKDIR /app
COPY --from=build /opt/java-minimal /opt/java
COPY --from=build --chown=app:app /app/target/outbox-demo-0.0.1-SNAPSHOT.jar app.jar
USER app
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
