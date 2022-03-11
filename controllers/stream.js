const express = require("express");
const fs = require('fs');
const { pipeline } = require('stream');
const axios = require("axios").default;
const axiosRetry = require("axios-retry");
const pub_sub_svcs = require('.././services/pub-sub.js');
const utils = require('.././services/utils.js');
const bq_svcs = require('.././services/bq.js');
const gcp_infra_svcs = require('.././services/gcp-infra.js');

const config = require('../config.js');
const https = require('https');

const router = express.Router();

axiosRetry(axios, {
  retries: 3,
  retryDelay: axiosRetry.exponentialDelay,
  shouldResetTimeout: true,
  retryCondition: (axiosError) => {
    return true;
  },
});

router.get("/", function (req, res) {
  gcp_infra_svcs.provisionDB().then(function (status) {
    if (status != null && status.includes('Successfully provisioned')) {
        gcp_infra_svcs.setupMsgInfra().then(function (statusMsg)    {
            if (statusMsg != null && statusMsg.includes(config.gcp_infra.topicName)) {
              streamTweetsHttp();
            }        
        })
        res.send("Now streaming tweets with new GCP infra ..");
    }

}).catch(error => {
    streamTweetsHttp();
    res.send("Now streaming tweets with existing GCP infra ..");
})
});

router.get("/clean", function (req, res) {
  gcp_infra_svcs.cleanUp();
  res.send('GCP resources deleted');
});

router.get("/alive", function (req, res) {
  //console.log('staying alive ..');
  res.send('Alive');
});

router.get("/engagement", function (req, res) {
  bq_svcs.queryRecentTweetsforEngagement().then(function (tweets) {
    console.log('Tweets array size ', tweets.length);
    updateEngagementMetrics(tweets);
    res.send("Updating engagement metrics ..");
  })
});

async function updateEngagementMetrics(tweets) {
  let axiosConfig = {
    headers: { Authorization: config.power_track_api.bearer_token, 'Content-Type': 'application/json', 'Accept-Encoding': 'gzip' }
  };
  let tweet_ids = [];
  let quotient = Math.floor(tweets.length / 250)
  var pointer = 0;
  let mod = tweets.length % 250
  tweets.forEach(tweet => {
    tweet_ids.push(tweet['id']);
  });
  for (var i = 0; i < quotient; i++) {
    let batch_tweet_ids = tweet_ids.slice(pointer, pointer + 249)
    let bodyParameters = {
      'tweet_ids': batch_tweet_ids,
      'engagement_types': ['video_views', 'replies', 'favorites', 'quote_tweets', 'retweets'],
      'groupings': { 'group': { 'group_by': ['tweet.id', 'engagement.type'] } }
    };
    axios.post(
      config.engagement_api.totals_endpoint,
      bodyParameters,
      axiosConfig
    )
      .then(function (engageObj) {
        //console.log('batch_tweet_ids ',batch_tweet_ids);
        unavail_tweet_list = engageObj.data.unavailable_tweet_ids;
        unavail_tweet_list.forEach(function (value, index) {
          for (let j = 0; j < batch_tweet_ids.length; j++) {
            if (value === batch_tweet_ids[j]) {
              batch_tweet_ids.splice(j, 1);
            }
          }
        })
        var tweet_engage_list= [];
        for (let k = 0; k < batch_tweet_ids.length; k++) {
          //console.log('Engage ', k, ' Tweet ',batch_tweet_ids[k], ' engage ', engageObj.data.group[batch_tweet_ids[k]]);
          engageObj.data.group[batch_tweet_ids[k]].id = batch_tweet_ids[k];
          tweet_engage_list.push(engageObj.data.group[batch_tweet_ids[k]]);
        }
        bq_svcs.insertEngagements(tweet_engage_list);
      })
      .catch(console.log);
    if (i === (quotient - 1))
      pointer = mod + pointer
    else
      pointer = pointer + 250;
    await utils.sleep(config.engagement_api.delay);
  }

}

router.get("/poll/:frequency/:delay", function (req, res) {
  console.log('polling Tweets from PubSub ', req.params.frequency);
  for (var i = 0; i < req.params.frequency; i++) {
    setTimeout(() => {
      pub_sub_svcs.synchronousPull(config.gcp_infra.projectId, config.gcp_infra.subscriptionName, config.gcp_infra.messageCount).then((messenger) => {

        if (messenger === 'disconnect') {
          console.log('Stream reconnecting => ', messenger);
          streamTweetsHttp();
        }
      })

    }, req.params.delay);
  }
  res.send('polling Tweets from PubSub');
});

async function streamTweetsHttp() {

  var options = {
    host: config.power_track_api.pt_stream_host,
    port: 443,
    path: config.power_track_api.pt_stream_path,
    keepAlive: true,
    headers: {
      'Authorization': 'Basic ' + new Buffer(config.power_track_api.gnip_username + ':' + config.power_track_api.gnip_password).toString('base64')
    }
  };
  request = https.get(options, function (res) {
    console.log('streaming with HTTP .. ', config.app_name);
    var body = '';
    res.on('data', function (data) {
      // our stream will only emit a single JSON root node.
      var splited_payload = '';
      //console.log('got data: ', data.toString(),'---------\n');
      var json_payload = data.toString();
      if (json_payload) {
        try {
          JSON.parse(json_payload);
          pub_sub_svcs.publishMessage(config.gcp_infra.topicName, JSON.stringify(json_payload));
        } catch (e) {
          //console.log('Error -- ',e.message);
          if (json_payload[0] === undefined || json_payload[0] === '\r' || json_payload[0] === '' || json_payload[0] === '\n') {
            console.log('~~~ Heartbeat payload ~~~ ');
          } else {
            if (splited_payload.length > 0) {
              splited_payload.append(json_payload);
              pub_sub_svcs.publishMessage(JSON.stringify(splited_payload));
              console.log('splited_payload ', JSON.parse(splited_payload));
              splited_payload = '';
            }
            else
              splited_payload = json_payload;
          }
        }
      }
    });
    res.on('end', function () {
      //here we have the full response, html or json object
      console.log(body);
    })
    res.on('error', function (e) {
      console.log("Got error: " + e.message);
      streamTweetsHttp();
    });
  });
}

module.exports = router
module.exports.streamTweetsHttp = streamTweetsHttp;
