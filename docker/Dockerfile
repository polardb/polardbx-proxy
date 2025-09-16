FROM polardbx-opensource-registry.cn-beijing.cr.aliyuncs.com/polardbx/polardbxbasejava:20221129

WORKDIR /home/admin

COPY proxy-server/target/polardbx-proxy polardbx-proxy
COPY docker/entrypoint.sh entrypoint.sh

RUN echo "admin:admin" | chpasswd && \
    echo "admin    ALL=(ALL)    NOPASSWD: ALL" >> /etc/sudoers

RUN chmod a+x entrypoint.sh && \
    chown admin:admin -R /home/admin && \
    true

USER admin

# Set command to entrypoint.sh
ENTRYPOINT /home/admin/entrypoint.sh
