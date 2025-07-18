
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

FROM csighub.tencentyun.com/emr/kona-jdk-17:17.0.7.b1 AS builder

# Add the entire project to the build container, unzip it,
# and remove flink-optimizer to reduce the container size.

ADD . /workspace/amoro
WORKDIR /workspace/amoro

RUN set -ex && \
    yum install -y kernel-devel && \
    yum install -y tlinux-release-scl && \
    yum install -y scl-utils && \
    rpm -ivh http://mirrors.tencent.com/tlinux/2.6/updates/x86_64/Packages/kernel-debug-devel-3.10.0-957.21.3.el7.x86_64.rpm && \
    yum install -y shadow-utils.x86_64 && \
    yum install -y vim && \
    yum install -y vi && \
    yum install -y less && \
    yum install -y procps && \
    yum install -y mysql && \
    yum install -y mysql-devel && \
    yum install -y net-tools && \
    yum install -y iputils && \
    yum install -y wget && \
    yum install -y tar && \
    yum install -y crontabs && \
    yum install -y hostname && \
    yum install -y bash && \
    yum install -y ntpdate &&  \
    yum install -y lsof && \
    mkdir -p /usr/local/service/ && \
    rm -f /etc/profile && \
    mkdir -p ${AMORO_HOME} && \
    mkdir -p  && \
    mkdir -p /var/spool/cron

RUN wget -q -O /usr/local/service/tools/security_pw https://emr-image-pipeline-1259353343.cos.ap-shanghai.myqcloud.com/tools/security_pw && \
    chmod 755 /usr/local/service/tools/security_pw

RUN AMORO_VERSION=`cat pom.xml | grep 'amoro-parent' -C 3 | grep -Eo '<version>.*</version>' | awk -F'[><]' '{print $3}'` \
    && cp dist/target/*.tar.gz /usr/local \
    && tar -xzvf /usr/local/apache-amoro-${AMORO_VERSION}-bin.tar.gz -C /usr/local \
    && rm /usr/local/amoro-${AMORO_VERSION}/plugin/optimizer/flink -rf \
    && rm /usr/local/amoro-${AMORO_VERSION}/plugin/optimizer/spark -rf \
    && mv /usr/local/amoro-${AMORO_VERSION} ${AMORO_HOME} \
    && rm -rf /workspace/amoro


FROM csighub.tencentyun.com/emr/kona-jdk-17:17.0.7.b1 AS builder

ARG MAVEN_MIRROR=https://repo.maven.apache.org/maven2

ENV AMORO_HOME /usr/local/service/amoro
ENV AMORO_CONF_DIR ${AMORO_HOME}/conf
ENV LOG_LEVEL info
EXPOSE 1630 1260 1261

COPY ./docker/amoro/tclake/entrypoint.sh /
RUN chmod +x /entrypoint.sh
COPY --from=builder ${AMORO_HOME} ${AMORO_HOME}

WORKDIR ${AMORO_HOME}

RUN cd ${AMORO_HOME}/lib \
    && wget ${MAVEN_MIRROR}/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar

ENTRYPOINT ["/entrypoint.sh"]
CMD ["help"]