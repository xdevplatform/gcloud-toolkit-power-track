var config = {};

config.PORT = 4080;

config.power_track_api  =   {
    'pt_stream_host' : 'gnip-stream.twitter.com',
    'pt_stream_path' : '/stream/powertrack/accounts/{{account}}/publishers/twitter/{{stream}}.json',
    'gnip_username' : '<<USERNAME>>',
    'gnip_password' : '<<PASSWORD>>',
    'bearer_token' : 'Bearer <<YOUR_BEARER_TOKEN_HERE>>'
}

config.search_api  =   {
    'pt_stream_host' : 'gnip-api.twitter.com',
    'pt_stream_path' : '/search/fullarchive/accounts/Prasanna-Selvaraj/dev.json',
    'gnip_username' : '<<USERNAME>>',
    'gnip_password' : '<<PASSWORD>>'
}

config.gcp_infra = {
    "projectId" : "<<PROJECT_ID>>",
    "topicName" : "<<TOPIC_NAME>>",
    "subscriptionName" : "<<SUBSCRIPTION_NAME>>",
    "messageCount" : 500,
    "streamReconnectCounter" : 3
}
config.gcp_infra.bq = {
    "dataSetId": "<<DATASET_ID>>"
}
config.gcp_infra.bq.table = {
    "tweets": "tweets",
    "engagement": "engagement",
    "search" : "search",
    "search_counts" : "search_counts"
}

config.engagement_api = {
    "totals_endpoint" : "https://data-api.twitter.com/insights/engagement/totals",
    "delay" : 600,
    "minMinutes" : 60,
    "maxMinutes" : 1440
}

config.app_name = 'trends-dev'

module.exports = config;
