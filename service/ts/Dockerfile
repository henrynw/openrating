FROM node:20-slim
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci || npm i
COPY tsconfig.json .
COPY src ./src
RUN npm run build
EXPOSE 8080
CMD ["npm","start"]
