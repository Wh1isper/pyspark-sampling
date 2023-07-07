FROM wh1isper/pyspark-app-base:3.4.1
LABEL maintainer="wh1isper <9573586@qq.com>"

COPY --chown=9999:9999 . /home/application
RUN pip3 install -e ./sparksampling_proto && pip3 install -e .[test]


USER application

EXPOSE 8530
ENTRYPOINT ["sparksampling"]

# docker build -t wh1isper/pysparksampling-dev:latest -f dockerbuild/pysparksampling-dev.Dockerfile . --no-cache
