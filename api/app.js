const express = require('express');
const app = express();
const port = process.env.PORT || 3000;

// Middleware to parse JSON requests
app.use(express.json());

// Define a route to accept events (POST request)
app.post('/events', (req, res) => {
  const eventData = req.body; // Assuming event data is sent in the request body
  console.log('Received event:', eventData);
  // You can process the event data here
  res.status(200).send('Event received successfully');
});

// Start the server
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});

