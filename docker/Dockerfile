FROM centos:6

# Update repository to use CentOS vault.
COPY CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo

RUN yum -y update
RUN yum -y install bison \
                   cmake \
                   flex \
                   gcc-c++ \
                   glibc-langpack-en \
                   libev-devel \
                   openssl-devel \
                   readline-devel \
                   make \
                   redhat-rpm-config
