FROM alpine:latest AS refit

RUN apk --update --no-cache add wget 7zip && rm -rf /var/cache/apk/*

WORKDIR /refit

RUN wget https://pureportal.strath.ac.uk/files/52873459/Processed_Data_CSV.7z && \
    7z x Processed_Data_CSV.7z && \
    rm Processed_Data_CSV.7z

FROM node:20.9.0

RUN apt-get update && apt-get install -y \
  dumb-init \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY docker/run.sh /app
RUN chmod +x /app/run.sh

COPY package.json /app
COPY package-lock.json /app
COPY example_data /app/example_data
COPY --from=refit /refit/*.csv /app/example_data

RUN npm install --production

COPY index.js /app

CMD ["dumb-init", "./run.sh"]