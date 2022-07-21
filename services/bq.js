const { BigQuery } = require("@google-cloud/bigquery");
const config = require('../config.js');
const utils = require('./utils.js');

async function insertRowsAsStream(rows, tableName) {
  const bigqueryClient = new BigQuery();

  // Insert data into a table
  try {
    const result = await new Promise((resolve, reject) => {
      bigqueryClient
        .dataset(config.gcp_infra.bq.dataSetId)
        .table(tableName)
        .insert(rows)
        .then((results) => {
          console.log(config.app_name, '-- table -- ', tableName, ` Inserted ${rows.length} rows`);
          resolve(rows);
        })
        .catch((err) => {
          reject(err);
        });
    });
  } catch (error) {
    console.log("----BQ JSON Error --- \n ", JSON.stringify(error), "\n");
    throw new Error(error);
  }
}

async function insertResults(results, category, isSearch) {
  var resultRows = [];
  results.forEach(function (tweet, index) {
    //console.log(' -- TWEET -- ',tweet);

    if (tweet) {
      if( isSearch === false)
        tweet = JSON.parse(tweet);
      if (tweet.geo != undefined) {
        var geoVar = tweet.geo;
        if (tweet.geo.coordinates != undefined) {
          geoVar.coordinates = tweet.geo.coordinates;
          if (tweet.geo.coordinates.coordinates != undefined && Array.isArray(tweet.geo.coordinates.coordinates) && tweet.geo.coordinates.coordinates.length) {
            geoVar.coordinates.coordinates = tweet.geo.coordinates.coordinates;
          }
        }
      }

      var tweet_type;
      // Determine Tweet type
      if (tweet.in_reply_to_user_id != undefined || tweet.in_reply_to_user_id != null) {
        tweet_type = 'Reply';
      } else if (tweet.text) {
        if (tweet.text.startsWith('RT', 0)) {
          tweet_type = 'Retweet'
        }
      } else if (tweet.quoted_status) {
        tweet_type = 'Quote'
      }

      if (tweet_type === null || tweet_type === undefined) {
        tweet_type = 'Original'
      }

      // Determine full text
      if (tweet.extended_tweet) {
        tweet.text = tweet.extended_tweet.full_text;
      }

      if (tweet.entities != undefined) {
        var entitiesVar = tweet.entities;
        if (tweet.entities.urls === undefined)
          entitiesVar.urls = [];
        else
          entitiesVar.urls = [];
        // TODO: ^^ correct it
        if (tweet.entities.user_mentions.length == 0) {
          tweet.entities.user_mentions = entitiesVar.user_mentions;
          entitiesVar.user_mentions.push({ 'id': 0, 'id_str': '0', 'name': 'NA', 'screen_name': 'NA', 'indices': [0] })
        }
        if (tweet.entities.hashtags.length == 0) {
          entitiesVar.hashtags = tweet.entities.hashtags;
          entitiesVar.hashtags.push({ 'text': 'NA', 'indices': [0] })
        }
        if (tweet.entities.media === undefined)
          entitiesVar.media = [];
        entitiesVar.media = [];
      }

      // Determine Media
      if (tweet.media != undefined) {
        var media = tweet.media;
        var mediaArr = getMediaArray(media);
      }
      if (tweet.extended_entities != undefined) {
        var e_media = tweet.extended_entities.media;
        var e_mediaArr = getMediaArray(e_media);
        var extended_entities = {};
        extended_entities.media = e_mediaArr;
      }

      if (tweet.user != undefined) {
        tweet.user.user_url = 'http://twitter.com/' + tweet.user.screen_name
        if (tweet.user.derived === undefined) {
          tweet.user.derived = { 'locations': [] }
          tweet.user.derived.locations.push({ 'country': 'NA', 'country_code': 'NA' })
        } if(tweet.user.derived.locations.geo != undefined)  {
          tweet.user.derived.locations.geo = {};
        }
      }

      if (tweet.created_at != undefined) {
        var cDate = new Date(tweet.created_at);
        var tweety = {};
        tweety.id = tweet.id_str
        tweety.text = tweet.text
        tweety.category = category
        tweety.reply_settings = tweet.reply_settings
        tweety.source = tweet.source
        tweety.author_id = tweet.author_id
        tweety.conversation_id = tweet.conversation_id
        tweety.created_at = BigQuery.datetime(cDate.toISOString())
        tweety.lang = tweet.lang
        tweety.in_reply_to_user_id = tweet.in_reply_to_user_id
        tweety.in_reply_to_screen_name = tweet.in_reply_to_screen_name
        tweety.possibly_sensitive = tweet.possibly_sensitive
        tweety.geo = geoVar
        tweety.entities = entitiesVar
        tweety.user = tweet.user
        tweety.tweet_url = 'http://twitter.com/twitter/status/' + tweet.id_str
        tweety.tweet_type = tweet_type
        if (tweet.media != undefined || tweet.media != null)
          tweety.media = mediaArr
        if (tweet.extended_entities != undefined || tweet.extended_entities != null)
          tweety.extended_entities = extended_entities
        //console.log('====== pushed tweet id ',tweet.id, 'type ', tweet_type);
        resultRows.push(tweety);
      }
    }
  });
  let tableName;
  if(isSearch === false)
    tableName = config.gcp_infra.bq.table.tweets
  else
  tableName = config.gcp_infra.bq.table.search
  if (resultRows.length > 0)
    insertRowsAsStream(resultRows, tableName);
}

function getMediaArray(media) {
  var mediaArr = [];
  media.forEach(function (media, index) {
    var mediaObj = {};
    mediaObj.id = media.id_str;
    mediaObj.media_url = media.media_url;
    mediaObj.media_url_https = media.media_url_https;
    mediaObj.url = media.url;
    mediaObj.display_url = media.display_url;
    mediaObj.expanded_url = media.expanded_url;
    mediaObj.type = media.type;
    mediaArr.push(mediaObj);
  })
  return mediaArr;
}

async function queryRecentTweetsforEngagement() {
  const bigqueryClient = new BigQuery();
  let tableName = config.gcp_infra.bq.dataSetId + '.' + config.gcp_infra.bq.table.tweets;
  console.log('queryBQTable SQL ', utils.getEngagementSQL(tableName, config.engagement_api.minMinutes, config.engagement_api.maxMinutes));
  const options = {
    query: utils.getEngagementSQL(tableName, config.engagement_api.minMinutes, config.engagement_api.maxMinutes),
    location: 'US',
  };

  const [rows] = await bigqueryClient.query(options);
  console.log('Query Results: ', rows.length);
  return rows;
}

async function insertEngagements(results) {
  results.forEach(function (engage, index) {
    engage.created_at = BigQuery.datetime(new Date().toISOString());
  })
  if (results.length > 0)
    insertRowsAsStream(results, config.gcp_infra.bq.table.engagement);
}

async function insertFollowers(users, parentHandle) {
  var resultRows = [];
  users.forEach(function (user, index) {
    if( user )  {
      user.parent_handle = parentHandle;
      user.followers_added_time	= BigQuery.datetime(new Date().toISOString());
      user.user_url = 'http://twitter.com/' + user.screen_name
      resultRows.push(user);
    }
  });
  insertRowsAsStream(resultRows, config.gcp_infra.bq.table.followers);
}

module.exports = { insertResults, queryRecentTweetsforEngagement, insertEngagements, insertFollowers };
