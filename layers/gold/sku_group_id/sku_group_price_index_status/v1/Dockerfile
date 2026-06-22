FROM python:3.9.13 AS poetry

WORKDIR /src
COPY . .

WORKDIR /src/layers/gold/sku_group_id/sku_group_price_index_status/v1
RUN pip install poetry && poetry build -f wheel

FROM python:3.9.13

ARG spark_uid=185
ARG NEXUS_USERNAME
ARG NEXUS_PASSWORD
ENV DEBIAN_FRONTEND=noninteractive

RUN groupadd --system --gid=${spark_uid} spark && \
    useradd --system --uid=${spark_uid} --gid=spark spark

RUN set -ex && \
    apt-get update && \
    ln -s /lib /lib64 && \
    apt install -qq -y gnupg2 wget bash tini libc6 libpam-modules krb5-user libnss3 procps net-tools gosu python3 python3-pip && \
    mkdir -p /opt/spark && \
    mkdir -p /opt/cert && \
    mkdir -p /opt/spark/examples && \
    mkdir -p /opt/spark/work-dir && \
    touch /opt/spark/RELEASE && \
    chown -R spark:spark /opt/spark && \
    chown -R spark:spark /opt/cert && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    rm -rf /var/cache/apt/* && \
    rm -rf /var/lib/apt/lists/*

ARG SPARK_VERSION=3.4.1
ENV SPARK_TGZ_URL=https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    SPARK_TGZ_ASC_URL=https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-${SPARK_VERSION}-bin-hadoop3.tgz.asc

ENV JAVA_GZ_URL=https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.11%2B9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.11_9.tar.gz

RUN set -ex; \
    export SPARK_TMP="$(mktemp -d)"; \
    cd "$SPARK_TMP"; \
    wget -nv -O spark.tgz "$SPARK_TGZ_URL"; \
    wget -nv -O spark.tgz.asc "$SPARK_TGZ_ASC_URL"; \
    tar -xf spark.tgz --strip-components=1; \
    chown -R spark:spark .; \
    mv jars /opt/spark/; \
    mv bin /opt/spark/; \
    mv sbin /opt/spark/; \
    mv kubernetes/dockerfiles/spark/decom.sh /opt/; \
    mv examples /opt/spark/; \
    mv kubernetes/tests /opt/spark/; \
    mv data /opt/spark/; \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws.jar; \
    curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.348/aws-java-sdk-bundle-1.12.348.jar -o /opt/spark/jars/aws-java-sdk-bundle.jar; \
    cd ..; \
    rm -rf "$SPARK_TMP"; \
    export JAVA_TMP="$(mktemp -d)"; \
    cd "$JAVA_TMP"; \
    wget -O OpenJDK.tar.gz "$JAVA_GZ_URL"; \
    tar xzf OpenJDK.tar.gz; \
    chown -R spark:spark .; \
    mv jdk-11.0.11+9 /opt/java/; \
    cd ..; \
    rm -rf "$JAVA_TMP"

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java

COPY layers/gold/sku_group_id/sku_group_price_index_status/v1/entrypoint.sh /opt/

WORKDIR /opt/spark/work-dir
RUN chmod g+w /opt/spark/work-dir
RUN chmod a+x /opt/decom.sh
RUN chmod a+x /opt/entrypoint.sh

ENV NEXUS_USERNAME=$NEXUS_USERNAME
ENV NEXUS_PASSWORD=$NEXUS_PASSWORD

RUN wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" --output-document /opt/cert/YandexInternalRootCA.crt && \
    chmod 655 /opt/cert/YandexInternalRootCA.crt

COPY --from=poetry /src/layers/gold/sku_group_id/sku_group_price_index_status/v1/dist/*.whl /tmp/
RUN pip install /tmp/*.whl
RUN rm -f /tmp/*.whl

COPY layers/gold/sku_group_id/sku_group_price_index_status/v1/entrypoints/ /opt/spark/entrypoints

RUN /opt/java/bin/keytool -importcert \
             -alias YandexCA \
             -file "/opt/cert/YandexInternalRootCA.crt" \
             -keystore /opt/spark/work-dir/client.truststore \
             -storepass changeit \
             -storetype JKS \
             --noprompt

ENTRYPOINT ["/opt/entrypoint.sh"]
