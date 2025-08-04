FROM reg.docker.alibaba-inc.com/drds/drds-basic:2.0-5755639-59e3c1b3-20240812164911-x86
MAINTAINER drds <tddl_team@list.alibaba-inc.com>

# Install essential utils
#RUN wget https://dev.mysql.com/get/mysql80-community-release-el7-4.noarch.rpm && \
#    yum remove -y mysql && \
#    rpm -ivh  mysql80-community-release-el7-4.noarch.rpm && \
#    yum install -y mysql-community-client-8.0.31-1.el7.x86_64 --nogpgcheck && \
#    chown -R admin:admin /home/admin && \
#    chgrp -R admin /home/admin/

RUN yum install -y less --nogpgcheck

WORKDIR /home/admin

COPY proxy-server/target/polardbx-proxy polardbx-proxy
COPY docker/entrypoint.sh entrypoint.sh
#COPY docker/conf/env.properties env/env.properties

RUN echo "admin:admin" | chpasswd && \
    echo "admin    ALL=(ALL)    NOPASSWD: ALL" >> /etc/sudoers

RUN chmod a+x entrypoint.sh && \
    chown admin:admin -R /home/admin && \
    true

USER admin

# Set command to entrypoint.sh
ENTRYPOINT /home/admin/entrypoint.sh