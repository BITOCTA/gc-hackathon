const express = require('express');
const app = express();
const port = process.env.PORT || 3000;
const {PubSub, Message} = require('@google-cloud/pubsub');


// Middleware to parse JSON requests
app.use(express.json());

// Define a route to accept events (POST request)
app.post('/events', async (req, res) => {
  const eventData = req.body; // Assuming event data is sent in the request body
  console.log('Received event:', eventData);

  await sendEvent(eventData, res)

    console.log("gsgs")


  res.status(200)


});

// Start the server
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});

async function sendEvent(
  event,
  res,
  projectId = 'cloud-summit-waw23-1483',
  topicNameOrId = 'projects/cloud-summit-waw23-1483/topics/telemetry-data', // Name for the new topic to create
) {
  // Instantiates a client
  const pubsub = new PubSub({projectId});

  // Creates a new topic
  const topic = pubsub.topic(topicNameOrId);



  topic.publishMessage({data: Buffer.from(JSON.stringify(event))}, () => {
    console.log("message sent successfully")
  });

}

