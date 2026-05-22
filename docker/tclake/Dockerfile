
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.


# Usage:
#   Run the docker command below under project dir.
#      docker build \
#        --file docker/amoro/tclake/Dockerfile \
#        --tag apache/amoro:tagname
#        .

FROM mirrors.tencent.com/tlinux/tlinux4.0:latest AS builder

# Add the entire project to the build container, unzip it,
# and remove flink-optimizer to reduce the container size.

ADD . /workspace/amoro
WORKDIR /workspace/amoro

ENV SERVICE_HOME /usr/local/service
ENV AMORO_HOME ${SERVICE_HOME}/amoro

# TencentKona-17 JDK 安装路径，与旧基础镜像保持一致
ENV JAVA_HOME /usr/lib/jvm/TencentKona-17.0.7.b1
ENV PATH ${JAVA_HOME}/bin:${PATH}

ARG TENCENT_KONA_JDK_URL=https://github.com/Tencent/TencentKona-17/releases/download/TencentKona-17.0.7/TencentKona-17.0.7.b1-jdk_linux-x86_64.tar.gz

RUN set -ex && \
    yum install -y wget && \
    yum install -y tar && \
    yum install -y gzip && \
    mkdir -p ${SERVICE_HOME} && \
    mkdir -p ${SERVICE_HOME}/tools && \
    mkdir -p /usr/lib/jvm && \
    wget -q -O /tmp/TencentKona-17.tar.gz ${TENCENT_KONA_JDK_URL} && \
    tar -xzf /tmp/TencentKona-17.tar.gz -C /usr/lib/jvm && \
    rm -f /tmp/TencentKona-17.tar.gz && \
    java -version

RUN wget -q -O /usr/local/service/tools/security_pw https://emr-image-pipeline-1259353343.cos.ap-shanghai.myqcloud.com/tools/security_pw && \
    chmod 755 ${SERVICE_HOME}/tools/security_pw

RUN AMORO_VERSION=`cat pom.xml | grep 'amoro-parent' -C 3 | grep -Eo '<version>.*</version>' | awk -F'[><]' '{print $3}'` \
    && cp dist/target/*.tar.gz /usr/local \
    && tar -xzvf /usr/local/apache-amoro-${AMORO_VERSION}-bin.tar.gz -C /usr/local \
    && rm /usr/local/amoro-${AMORO_VERSION}/plugin/optimizer/flink -rf \
    && rm /usr/local/amoro-${AMORO_VERSION}/plugin/optimizer/spark -rf \
    && mv /usr/local/amoro-${AMORO_VERSION} ${AMORO_HOME} \
    && rm -rf /workspace/amoro


FROM mirrors.tencent.com/tlinux/tlinux4.0:latest

#ARG MAVEN_MIRROR=https://repo.maven.apache.org/maven2
ARG MAVEN_MIRROR=https://mirrors.tencent.com/nexus/repository/maven-public

ARG TENCENT_KONA_JDK_URL=https://github.com/Tencent/TencentKona-17/releases/download/TencentKona-17.0.7/TencentKona-17.0.7.b1-jdk_linux-x86_64.tar.gz

ENV JAVA_HOME /usr/lib/jvm/TencentKona-17.0.7.b1
ENV PATH ${JAVA_HOME}/bin:${PATH}

ARG TCLAKE_AMORO_EVENT_LISTENER_MIRROR=https://mirrors.tencent.com/repository/maven/tclake-snapshot/com/tencent/cloud/tclake/amoro-plugin/0.2-SNAPSHOT/amoro-plugin-0.2-20250905.021825-1.jar

ENV SERVICE_HOME /usr/local/service
ENV AMORO_HOME ${SERVICE_HOME}/amoro

ENV LOG_LEVEL info
EXPOSE 1630 1260 1261

COPY ./docker/tclake/entrypoint.sh /
RUN chmod +x /entrypoint.sh

RUN set -ex && \
    yum install -y \
        shadow-utils \
        vim \
        less \
        procps-ng \
        mysql \
        mysql-devel \
        net-tools \
        iputils \
        wget \
        tar \
        gzip \
        crontabs \
        hostname \
        bash \
        chrony \
        perl \
        lsof && \
    yum clean all && \
    rm -rf /var/cache/yum && \
    mkdir -p /var/spool/cron && \
    rm -f /etc/profile

# 安装 TencentKona-17 JDK 到 ${JAVA_HOME}
RUN set -ex && \
    mkdir -p /usr/lib/jvm && \
    wget -q -O /tmp/TencentKona-17.tar.gz ${TENCENT_KONA_JDK_URL} && \
    tar -xzf /tmp/TencentKona-17.tar.gz -C /usr/lib/jvm && \
    rm -f /tmp/TencentKona-17.tar.gz && \
    java -version

COPY --from=builder ${SERVICE_HOME} ${SERVICE_HOME}

WORKDIR ${AMORO_HOME}

RUN cd ${AMORO_HOME}/lib \
    && wget ${MAVEN_MIRROR}/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar

RUN mkdir -p ${AMORO_HOME}/plugin/event-listeners \
    && cd ${AMORO_HOME}/plugin/event-listeners  \
    && wget ${TCLAKE_AMORO_EVENT_LISTENER_MIRROR} \
    && chmod 644 ${AMORO_HOME}/plugin/event-listeners/*

ENTRYPOINT ["/entrypoint.sh"]
CMD ["help"]