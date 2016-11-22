FROM h3crd-wlan1.chinacloudapp.cn:5000/histclientsdataetl_basic:0802

RUN mkdir -p /workspace/histclientsdataetl_V1

RUN mkdir -p /workspace/logs

WORKDIR /workspace/histclientsdataetl_V1

ADD src /workspace/histclientsdataetl_V1

RUN ["mvn", "assembly:assembly"]

CMD ["java","-jar","-Duser.timezone=GMT+08","target/microservice_histclientsdataetl-1.0-SNAPSHOT-jar-with-dependencies.jar","production"]