# Use the official Node.js image as the base image
FROM node:18

# Set the working directory
WORKDIR /app

# Copy package.json and package-lock.json
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the application code
COPY . .

# Expose the port your Express app is running on (default is 3000)
EXPOSE 3000

# Command to run your Express app
CMD ["node", "app.js"]
