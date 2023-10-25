const express = require('express');
const app = express();
const port = process.env.PORT || 3000;
const { PubSub } = require('@google-cloud/pubsub');

const projectId = 'cloud-summit-waw23-1483';
const topicNameOrId = 'projects/cloud-summit-waw23-1483/topics/telemetry-data';


const pubsub = new PubSub({ projectId});
const topic = pubsub.topic(topicNameOrId);

app.use(express.json());

app.post('/events', async (req, res) => {
  const eventData = req.body;

  console.log('Received event:', eventData);

  try {
    await sendEvent(eventData);
    console.log("Event sent successfully to Pub/Sub");
    res.status(200).send('Event processed successfully');
  } catch (error) {
    console.error('Failed to send event:', error);
    res.status(500).send('Failed to process event');
  }
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});

const publishMessage = (message) => {
    return new Promise((resolve, reject) => {
      topic.publishMessage(message, (err, messageId) => {
        if (err) reject(err);
        else resolve(messageId);
      });
    });
  };

async function sendEvent(event) {
  const messageId = await publishMessage({ data: Buffer.from(JSON.stringify(event)) });
  console.log(`Message ${messageId} sent successfully`);
}

