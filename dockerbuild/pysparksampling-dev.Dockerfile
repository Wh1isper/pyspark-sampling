FROM wh1isper/pysparksampling-base:latest
LABEL maintainer="wh1isper <9573586@qq.com>"

WORKDIR /home/application
COPY --chown=9999:9999 . /home/application
RUN pip3 install -e .


USER application

EXPOSE 8530
ENTRYPOINT ["sparksampling"]

# docker build -t wh1isper/pysparksampling-dev:latest -f dockerbuild/pysparksampling-dev.Dockerfile . --no-cache
