function sleep(milliseconds) {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve('timed');
        }, milliseconds)
    })
}

function getEngagementSQL(tableName, minMinutes, maxMinutes) {
    return `SELECT id  
    FROM `+ tableName + ` WHERE DATETIME_DIFF(current_datetime, created_at, MINUTE) > `+ minMinutes + 
    ` AND DATETIME_DIFF(current_datetime, created_at, MINUTE) < ` + maxMinutes + ` AND TWEET_TYPE != 'Retweet'`;
}

module.exports = { sleep, getEngagementSQL };