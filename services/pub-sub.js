const {PubSub} = require('@google-cloud/pubsub');
const {v1} = require('@google-cloud/pubsub');
const { insertResults } = require('./bq');
const config = require('../config.js');

const pubSubClient = new PubSub();
var counter = 0;

async function publishMessage(topicName, message) {
  // Publishes the message as a string, e.g. "Hello, world!" or JSON.stringify(someObject)
  const dataBuffer = Buffer.from(message);

  try {
    const messageId = await pubSubClient.topic(topicName).publish(dataBuffer);
    //console.log(`Message ${messageId} published.`);
  } catch (error) {
    console.error(`Received error while publishing: ${error.message}`);
    process.exitCode = 1;
  }
}

const timeout = 60;

async function listenForMessages(subscriptionName) {
  // References an existing subscription
  const subscription = pubSubClient.subscription(subscriptionName);

  // Create an event handler to handle messages
  let messageCount = 0;
  const messageHandler = message => {
    console.log(`Received message ${message.id}:`);
    //console.log(`\tData: ${message.data}`);
    //console.log(`\tAttributes: ${message.attributes}`);
    messageCount += 1;

    // "Ack" (acknowledge receipt of) the message
    message.ack();
  };

  // Listen for new messages until timeout is hit
  subscription.on('message', messageHandler);

  setTimeout(() => {
    subscription.removeListener('message', messageHandler);
    console.log(`${messageCount} message(s) received.`);
  }, timeout * 100000);
}

// Creates a client; cache this for further use.
const subClient = new v1.SubscriberClient();

async function synchronousPull(projectId, subscriptionName, maxMessagesToPull) {
  const formattedSubscription = subClient.subscriptionPath(
    projectId,
    subscriptionName
  );

  // The maximum number of messages returned for this request.
  // Pub/Sub may return fewer than the number specified.
  const request = {
    subscription: formattedSubscription,
    maxMessages: maxMessagesToPull,
  };

  // The subscriber pulls a specified number of messages.
  const [response] = await subClient.pull(request);

  // Process the messages.
  const ackIds = [];
  var tweets = [];

  for (const message of response.receivedMessages) {
    //console.log('Received Message :- ',message.message.data.toString());
    tweets.push(JSON.parse(message.message.data.toString()));
    ackIds.push(message.ackId);
  }

  console.log(config.app_name,' Tweets pulled -- ',tweets.length);
  if( tweets.length == 0 )
    counter++;
  if( counter > config.gcp_infra.streamReconnectCounter )  {
    counter = 0;
    return new Promise(function (resolve, reject) {
      resolve('disconnect')
    });
  }
  // Insert into BQ
  await insertResults(tweets,config.app_name);

  if (ackIds.length !== 0) {
    // Acknowledge all of the messages. You could also ackknowledge
    // these individually, but this is more efficient.
    const ackRequest = {
      subscription: formattedSubscription,
      ackIds: ackIds,
    };

    await subClient.acknowledge(ackRequest);
  }
}

module.exports = { publishMessage, listenForMessages, synchronousPull };

