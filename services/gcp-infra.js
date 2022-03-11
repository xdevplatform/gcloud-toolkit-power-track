const { BigQuery } = require("@google-cloud/bigquery");
const { v1 } = require('@google-cloud/pubsub');
const { PubSub } = require('@google-cloud/pubsub');
const config = require('../config.js');
const bigquery = new BigQuery();
const fs = require('fs');

const pubSubClient = new PubSub();
const subClient = new v1.SubscriberClient();

async function provisionDataSet() {
    return new Promise(function (resolve, reject) {
        createDataSet(config.gcp_infra.bq.dataSetId).then((dataSetResponse) => {
            console.log('dataSetResponse ', dataSetResponse);
            resolve('Successfully provisioned dataset');
        }).catch(function (error) {
            console.log('Error provisioning dataset ', error);
            resolve('Successfully provisioned -- already exists');
        })
    })
}

async function provisionTables() {
    return new Promise(function (resolve, reject) {
        createTables(config.gcp_infra.bq.dataSetId).then((tablesResponse) => {
            console.log('tablesResponse ', tablesResponse);
            resolve('Successfully provisioned tables');
        }).catch(function (error) {
            console.log('Error provisioning tables ', error);
            reject({ "error": "Error Provisioning tables " });
        });

    })
}


async function setupMsgInfra() {
    return new Promise(function (resolve, reject) {
        createTopic(config.gcp_infra.topicName).then(() => {
            createSubscription(config.gcp_infra.topicName, config.gcp_infra.subscriptionName).then(() => {
                resolve(config.gcp_infra.topicName);
            });
        });
    })
}

async function cleanUp() {
    deleteDataSet(config.gcp_infra.bq.dataSetId);
    await deleteSubscription(config.gcp_infra.subscriptionName);
    deleteTopic(config.gcp_infra.topicName);

}

async function createDataSet(dataSetName) {

    const options = {
        location: 'US',
    };

    console.log('dataSetName -- ', dataSetName);
    //    Create a new dataset
    const [dataset] = await bigquery.createDataset(dataSetName, options);
    const dataSetId = dataset.id;
    console.log(`Dataset ${dataSetId} created.`);

}

async function createTables(datasetId) {
    //create tables
    const tweets_schema = fs.readFileSync('./schema/tweets.json');
    const [tweets_table] = await bigquery.dataset(datasetId).createTable(config.gcp_infra.bq.table.tweets, { schema: JSON.parse(tweets_schema), location: 'US' });
    console.log(`Table ${tweets_table.id} created.`);

    const engagement_schema = fs.readFileSync('./schema/engagement.json');
    const [engagement_table] = await bigquery.dataset(datasetId).createTable(config.gcp_infra.bq.table.engagement, { schema: JSON.parse(engagement_schema), location: 'US' });
    console.log(`Table ${engagement_table.id} created.`);
}

async function createTopic(topicName) {
    // Creates a new topic
    await pubSubClient.createTopic(topicName);
    console.log(`Topic ${topicName} created.`);
}

async function createSubscription(topicName, subscriptionName) {
    // Creates a new subscription
    await pubSubClient.topic(topicName).createSubscription(subscriptionName);
    console.log(`Subscription ${subscriptionName} created.`);
}

async function deleteTopic(topicName) {
    await pubSubClient.topic(topicName).delete();
    console.log(`Topic ${topicName} deleted.`);
}

async function deleteSubscription(subscriptionName) {
    await pubSubClient.subscription(subscriptionName).delete();
    console.log(`Subscription ${subscriptionName} deleted.`);
}

async function deleteDataSet(dataSetName) {
    const dataset = bigquery.dataset(dataSetName);
    dataset.delete({ force: true }, (err, apiResponse) => { });
    console.log()
}

module.exports = { provisionDataSet, provisionTables, setupMsgInfra, cleanUp };