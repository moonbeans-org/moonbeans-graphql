# Use an official Node.js runtime as the base image
FROM --platform=linux/amd64 node:lts-alpine

# Set the working directory in the container
WORKDIR /app

# Copy package.json and package-lock.json to the working directory
COPY package.json package-lock.json* ./

# Install dependencies
RUN npm install

# Copy the watcher
COPY . .

# Copy the .env
COPY ../../.env ./

# Run the Node.js script
CMD ["/bin/sh", "listingCleanup.sh"]