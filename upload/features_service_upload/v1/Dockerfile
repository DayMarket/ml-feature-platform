FROM ghcr.io/daymarket/spark:v3.5.5-scala2.12-java17-ubuntu-python3

ARG NEXUS_USERNAME
ARG NEXUS_PASSWORD

USER 0

RUN pip install --no-cache-dir \
    --extra-index-url "https://${NEXUS_USERNAME}:${NEXUS_PASSWORD}@nexus.infra.internal.daymarket.uz/repository/um-pypi/simple/" \
    ranking-python-client==3.0.4

RUN mkdir -p /opt/cert /opt/spark/work-dir && \
    curl -fsSL "https://storage.yandexcloud.net/cloud-certs/CA.pem" \
      --output /opt/cert/YandexInternalRootCA.crt && \
    keytool -importcert \
      -alias YandexCA \
      -file /opt/cert/YandexInternalRootCA.crt \
      -keystore /opt/spark/work-dir/client.truststore \
      -storepass changeit \
      -storetype JKS \
      --noprompt && \
    chown -R 185:185 /opt/cert /opt/spark/work-dir

USER 185
