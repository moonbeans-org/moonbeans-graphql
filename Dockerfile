# Use an official Node.js runtime as the base image
FROM --platform=linux/amd64 node:lts-alpine

# Set the working directory in the container
WORKDIR /app

# Copy package.json and package-lock.json to the working directory
COPY package.json package-lock.json* ./

# Install dependencies
RUN npm install

# Copy the chain indexers package
COPY chainIndexers ./chainIndexers

# Copy the watcher
COPY watcher.js ./

# Copy the .env
COPY .env ./

# Expose ports 8080 and 3000
EXPOSE 8080 3000

# Run the Node.js script
CMD ["node", "watcher.js"]