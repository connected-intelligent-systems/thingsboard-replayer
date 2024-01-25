FROM node:20.9.0

RUN apt-get update && apt-get install -y \
  dumb-init \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY docker/run.sh /app
RUN chmod +x /app/run.sh

COPY package.json /app
COPY package-lock.json /app

RUN npm install --production

COPY index.js /app

CMD ["dumb-init", "./run.sh"]