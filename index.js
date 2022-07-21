const express = require('express');
const bodyParser = require('body-parser')
const cors = require('cors')
const stream = require('./controllers/stream')
const search = require('./controllers/search')

const app = express();
const PORT = process.env.PORT || 4080;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.use(cors());
app.options('*', cors()) 
app.post('*', cors()) 
app.use('/stream',stream);
app.use('/search',search);

app.listen(PORT, ()=>   {
    console.log("App listening on port",PORT);
    //stream.streamTweetsHttp();
});

module.exports = app;
