var config = {};

config.PORT = 4080;

config.power_track_api  =   {
    'pt_stream_host' : 'gnip-stream.twitter.com',
    'pt_stream_path' : '<<GET_STREAM_PATH_FROM_GNIP_CONSOLE>> Example - /stream/powertrack/accounts/{accountName}/publishers/twitter/{stream}.json',
    'gnip_username' : '<<GNIP_USERNAME>>',
    'gnip_password' : '<<GNIP_PASSWORD>>',
    'bearer_token' : 'Bearer <<YOUR_BEARER_TOKEN_HERE>>'
}

config.gcp_infra = {
    "projectId" : "<<GCP_PROJECT_ID>>",
    "topicName" : "<<GCP_PUBSUB_TOPIC>>",
    "subscriptionName" : "<<GCP_PUBSUB_SUBSCRIPTION>>",
    "messageCount" : 500,
    "streamReconnectCounter" : 3
}
config.gcp_infra.bq = {
    "dataSetId": "<<GCP_DATASET>>"
}
config.gcp_infra.bq.table = {
    "tweets": "<<BIGQUERY_TRENDS_TABLE>>",
    "engagement": "<<BIGQUERY_ENGAGEMENT_TABLE>>"
}

config.engagement_api = {
    "totals_endpoint" : "https://data-api.twitter.com/insights/engagement/totals",
    "delay" : 600,
    "minMinutes" : 60,
    "maxMinutes" : 1440
}

config.app_name = 'trends-dev'

module.exports = config;
