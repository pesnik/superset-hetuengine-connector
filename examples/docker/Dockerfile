# Dockerfile for Apache Superset with HetuEngine connector
# Based on official Apache Superset image

FROM apache/superset:latest

USER root

# Install Java 11 (required for JDBC connectivity)
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install HetuEngine connector
RUN pip install --no-cache-dir superset-hetuengine-connector

# Copy HetuEngine JDBC driver
# NOTE: Place your hetuengine-jdbc.jar in the same directory as this Dockerfile
COPY hetuengine-jdbc.jar /opt/hetuengine-jdbc.jar
RUN chmod 644 /opt/hetuengine-jdbc.jar

# Optional: Copy custom Superset configuration
# COPY superset_config.py /app/pythonpath/superset_config.py

# Switch back to superset user
USER superset

# Expose Superset port
EXPOSE 8088

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8088/health || exit 1

# Default command
CMD ["gunicorn", "--bind", "0.0.0.0:8088", "--workers", "4", \
     "--worker-class", "gthread", "--threads", "2", "--timeout", "300", \
     "--limit-request-line", "0", "--limit-request-field_size", "0", \
     "superset.app:create_app()"]
