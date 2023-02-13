FROM node:12.21.0

WORKDIR /app

COPY package.json /app
COPY package-lock.json /app

RUN npm install --production

COPY index.js /app

CMD ["npm", "start"]
