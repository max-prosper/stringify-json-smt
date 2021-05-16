# Kafka Connect SMT to convert values to JSON strings
This lib implements Kafka connect SMT (Single Message Transformation) to
convert values of specified fields to JSON strings (stringify).

## Config
Use it in connector config file:
~~~json
...
"transforms": "stringify",
"transforms.stringify.type": "com.github.maxprosper.smt.stringifyjson.StringifyJson$Value",
"transforms.stringify.targetFields": "field1,field2",
...
~~~

Use dot notation for deeper fields (e. g. `level1.level2`).

## Install to Kafka Connect
After build copy file `target/stirngify-json-smt-0.0.3-jar-with-deps.jar`
to Kafka Connect container `` copying to its docker image or so.

It can be done adding this line to Dockerfile:
~~~Dockerfile
COPY ./target/stringify-json-smt-0.0.3-jar-with-deps.jar $KAFKA_CONNECT_PLUGINS_DIR
~~~

Or download current release:
~~~Dockerfile
RUN curl -fSL -o /tmp/plugin.tar.gz \
    https://github.com/max-prosper/stringify-json-smt/releases/download/0.0.3/stringify-json-smt-0.0.3.tar.gz && \
    tar -xzf /tmp/plugin.tar.gz -C $KAFKA_CONNECT_PLUGINS_DIR && \
    rm -f /tmp/plugin.tar.gz;
~~~

## Build release file
- Increment version in `pom.xml` (e.g. to `0.0.4`).
- Run build script: `./scripts/build-release.sh 0.0.4`.
- Take `*.tar.gz` file from `target` folder and publish it.