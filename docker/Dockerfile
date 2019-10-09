FROM quay.io/btison/openjdk18-openshift:1.6

USER 0

COPY target/repository /home/jboss/.m2/repository
RUN chown -R jboss:0 /home/jboss/.m2
RUN chmod -R 776 /home/jboss/.m2

ENV JAVA_APP_DIR=/deployments

EXPOSE 8080 8443

USER 185

COPY target/process-service-1.1.0-SNAPSHOT.jar /deployments/