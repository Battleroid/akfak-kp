FROM alpine

RUN apk update && apk add python3

COPY ./ /tmp/akfak
RUN cd /tmp/akfak && \
    pip install -r requirements.txt && \
    pip install .

ENTRYPOINT ["akfak-kp"]
