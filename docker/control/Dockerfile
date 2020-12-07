FROM debian:buster
MAINTAINER jake@apache.org

ENV LEIN_ROOT true
ENV HTTP_PROXY "http://127.0.0.1:7890"
# ENV HTTPS_PROXY "http://127.0.0.1:7890"
RUN echo "deb http://mirrors.163.com/debian/ buster main non-free contrib" > /etc/apt/sources.list
RUN echo "deb http://mirrors.163.com/debian/ buster-updates main non-free contrib" >> /etc/apt/sources.list
RUN echo "deb http://mirrors.163.com/debian/ buster-backports main non-free contrib" >> /etc/apt/sources.list
RUN echo "deb http://mirrors.163.com/debian-security/ buster/updates main non-free contrib" >> /etc/apt/sources.list

RUN echo "deb-src http://mirrors.163.com/debian/ buster main non-free contrib" >> /etc/apt/sources.list
RUN echo "deb-src http://mirrors.163.com/debian/ buster-updates main non-free contrib" >> /etc/apt/sources.list
RUN echo "deb-src http://mirrors.163.com/debian/ buster-backports main non-free contrib" >> /etc/apt/sources.list
RUN echo "deb-src http://mirrors.163.com/debian-security/ buster/updates main non-free contrib" >> /etc/apt/sources.list
RUN echo "151.101.108.133 raw.githubusercontent.com" >> /etc/hosts
RUN echo -e "nameserver 8.8.8.8\nnameserver 8.8.4.4\nnameserver 1.1.1.1\noptions edns0" > /etc/resolv.conf
RUN mkdir -p ~/.lein
RUN echo "{\n    :user {\n        :mirrors {\n            \"central\" {\n                :name \"aliyun\"\n                :url \"https://maven.aliyun.com/nexus/content/groups/public\"\n            }\n            \"clojars\" {\n                :name \"cnnic\"\n                :url \"https://mirrors.cnnic.cn/clojars/\"\n            }\n        }\n    }\n}" > ~/.lein/profiles.clj
#
# Jepsen dependencies
#
RUN apt-get -y -q update && \
    apt-get install -qy openjdk-11-jdk-headless \
    libjna-java \
    vim \
    emacs \
    git \
    htop \
    screen \
    pssh \
    curl \
    wget \
    gnuplot \
    graphviz \
    dos2unix

RUN wget https://gist.githubusercontent.com/ekexium/cf3426a8837b5cb723437875bc4164cb/raw/566c3a73c3fada1f8893ed5492305a520bc92034/lein && \
    mv lein /usr/bin && \
    chmod +x /usr/bin/lein && \
    lein self-install

# without --dev flag up.sh copies jepsen to these subfolders
# with --dev flag they are empty until mounted
COPY jepsen/jepsen /jepsen/jepsen/
RUN if [ -f /jepsen/jepsen/project.clj ]; then cd /jepsen/jepsen && lein install; fi
COPY jepsen /jepsen/

ADD ./bashrc /root/.bashrc
ADD ./init.sh /init.sh
RUN dos2unix /init.sh /root/.bashrc \
    && chmod +x /init.sh

CMD /init.sh
