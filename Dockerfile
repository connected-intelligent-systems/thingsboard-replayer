FROM node:12.21.0

WORKDIR /app

COPY package.json /app
COPY package-lock.json /app
COPY example_data /app/example_data

RUN npm install --production

ENV CSV_FILE=./example_data/household_example.csv

COPY index.js /app

CMD ["npm", "start"]
