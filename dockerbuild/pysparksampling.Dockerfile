FROM wh1isper/pysparksampling-base:latest
LABEL maintainer="wh1isper <9573586@qq.com>"

WORKDIR /home/application
# install using pypi
RUN pip3 install sparksampling

USER application

EXPOSE 8530
ENTRYPOINT ["sparksampling"]

# docker build -t wh1isper/pysparksampling:latest -f dockerbuild/pysparksampling.Dockerfile . --no-cache
