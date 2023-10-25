FROM alpine:latest AS refit

RUN apk --update --no-cache add wget 7zip && rm -rf /var/cache/apk/*

WORKDIR /refit

RUN wget https://pureportal.strath.ac.uk/files/52873459/Processed_Data_CSV.7z && \
    7z x Processed_Data_CSV.7z && \
    rm Processed_Data_CSV.7z

FROM node:12.21.0

WORKDIR /app

COPY package.json /app
COPY package-lock.json /app
COPY example_data /app/example_data
COPY --from=refit /refit/*.csv /app/example_data

RUN npm install --production

COPY index.js /app

CMD ["npm", "start"]
