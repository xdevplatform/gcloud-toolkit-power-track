const express = require("express");
const axios = require("axios").default;
const axiosRetry = require("axios-retry");
const config = require('../config.js');
const bq_svcs = require('.././services/bq.js');
const gcp_infra_svcs = require('.././services/gcp-infra.js');
const utils = require('.././services/utils.js');

axiosRetry(axios, {
    retries: 3,
    retryDelay: axiosRetry.exponentialDelay,
    shouldResetTimeout: true,
    retryCondition: (axiosError) => {
        return true;
    },
});

const router = express.Router();

router.get("/", function (req, res) {
    res.send("Twitter Enterprise API Search Application");
});

router.post("/", function (req, res) {
    gcp_infra_svcs.provisionDataSet().then(function (status) {
        console.log('DB provisioning status ', status);
        if (status != null && status.includes('Successfully provisioned')) {
            gcp_infra_svcs.provisionTables().then(function (status) {
                if (status != null && status.includes('Successfully provisioned')) {
                    fullArchiveSearch(req.body).then(function (response) {
                        res.status(200).send(response);
                    }).catch(function(error) {
                        console.log("Error in FAS ",error)
                    });
                }
            });
        }
    });
});

async function fullArchiveSearch(reqBody, nextToken) {
    var fas = reqBody.fullArchiveSearch;
    var query = { "query": fas.query, "maxResults": 500, fromDate: fas.fromDate, toDate: fas.toDate }
    if (nextToken != undefined && nextToken != null)
        query.next = nextToken;
    return new Promise(function (resolve, reject) {
        let axiosConfig = {
            method: 'post',
            url: 'https://'+config.search_api.pt_stream_host + config.search_api.pt_stream_path,
            auth: {
                username: config.search_api.gnip_username,
                password: config.search_api.gnip_password
            },
            data: query
        };
        console.log('query ', JSON.stringify(query));
        axios(axiosConfig)
            .then(function (resp) {
                if (resp != null) {
                    console.log('Search results into BQ ');
                    if (resp.data != null && resp.data.results != null && resp.data.results.length > 0) {
                        bq_svcs.insertResults(resp.data.results, fas.category, true);
                    }
                    if (resp.data != undefined && resp.data.next != undefined) {
                        fullArchiveSearch(reqBody, resp.data.next);
                    } else {
                        // no next token - end of FAS insert followers
                    }
                    resolve({ "message": "Query result persisted" });
                }
            })
            .catch(function (error) {
                console.log('ERROR --- ', error);
                resolve(error);
            });
    });
}

module.exports = router;
