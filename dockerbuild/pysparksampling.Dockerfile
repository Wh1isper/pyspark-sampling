FROM wh1isper/pyspark-app-base:3.4.1
LABEL maintainer="wh1isper <9573586@qq.com>"
# install using pypi
RUN pip3 install sparksampling
EXPOSE 8530
ENTRYPOINT ["sparksampling"]

# docker build -t wh1isper/pysparksampling:latest -f dockerbuild/pysparksampling.Dockerfile . --no-cache
