FROM alpine

RUN apk update && apk add python3

COPY ./ /tmp/akfak
RUN cd /tmp/akfak && \
    pip3 install -r requirements.txt && \
    pip3 install .

ENTRYPOINT ["akfak-kp"]
