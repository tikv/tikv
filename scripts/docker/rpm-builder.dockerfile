FROM centos:7.6.1810 as builder

# Install the system dependencies
# Attempt to clean and rebuild the cache to avoid 404s
RUN yum clean all && \
    yum makecache && \
	yum update -y && \
	yum install -y make rpmbuild rpmdevtools git && \
	yum clean all
RUN echo '%_topdir     %{getenv:HOME}/rpmbuild' > $HOME/.rpmmacros
RUN rpmdev-setuptree