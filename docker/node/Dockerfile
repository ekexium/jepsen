# See https://github.com/jgoerzen/docker-debian-base
# See https://hub.docker.com/r/jgoerzen/debian-base-standard
FROM jgoerzen/debian-base-standard:buster

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
# I think this is a bug--debian-base-setup crashes because policy-rc.d isn't
# present in this image, and if you create it, exim crashes... do we actually NEED this? Maybe not...
#RUN mkdir /usr/sbin/policy-rc.d
#RUN run-parts --exit-on-error --verbose /usr/local/debian-base-setup

# Basic system stuff
RUN apt-get install -y apt-transport-https

# Install packages
RUN apt-get -qy update && \
    apt-get -qy install \
        dos2unix \
        openssh-server \
        pwgen

# When run, boot-debian-base will call this script, which will copy the
# authorized_keys file into root's .ssh
ADD setup-authorized-keys.sh /usr/local/preinit/03-setup-authorized-keys
RUN chmod +x /usr/local/preinit/03-setup-authorized-keys

# Configure SSHD
RUN sed -i "s/#PermitRootLogin prohibit-password/PermitRootLogin yes/g" /etc/ssh/sshd_config

# Enable SSH server
ENV DEBBASE_SSH enabled

# Install Jepsen deps
RUN apt-get install -qy build-essential bzip2 curl faketime iproute2 iptables iputils-ping libzip4 logrotate man man-db net-tools ntpdate psmisc python rsyslog sudo tar unzip vim wget ca-certificates

EXPOSE 22
CMD ["/usr/local/bin/boot-debian-base"]
