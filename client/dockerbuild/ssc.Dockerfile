FROM wh1isper/pysparksampling-base:latest
LABEL maintainer="wh1isper <9573586@qq.com>"

WORKDIR /home/application
# install using pypi
RUN pip3 install sparksampling_client

COPY --chown=application:application client/dockerbuild/config.json /home/application/.ssc/config.json
USER application

ENTRYPOINT ["ssc"]

# docker build -t wh1isper/pysparksampling_client:latest -f client/dockerbuild/ssc.Dockerfile . --no-cache
