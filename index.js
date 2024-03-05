const express = require("express");
const { MongoClient, ServerApiVersion } = require('mongodb');
require('dotenv').config()

const { fetchDataNSE, fetchDataBSE } = require("./helpers");
const { NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX } = require("./constants");

const app = express();
const port = process.env.PORT || 3001;

const {DB_USER, DB_PASSWORD} = process.env;

const DB_URI = `mongodb+srv://${DB_USER}:${DB_PASSWORD}@cluster0.lwkimnl.mongodb.net/?retryWrites=true&w=majority`;

const client = new MongoClient(DB_URI, {
    serverApi: {
        version: ServerApiVersion.v1,
        strict: true,
        deprecationErrors: true,
    }
});

const now = new Date();
const html = `Done ${now}`;

app.get("/nse/:indexName", async (req, res) => {
    try {
        await client.connect();

        if ([NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY].includes(req.params.indexName)) {
            await fetchDataNSE(req.params.indexName, client);
        }

        res.type('html').send(html);
    } catch (error) {
        console.log(error);
    } finally {
        await client.close();
    }
});

app.get("/bse/sensex", async (req, res) => {
    try {
        await client.connect();

        if (req.query.expiryDate && req.query.expiryDate.length === 8)
            await fetchDataBSE(SENSEX, req.query.expiryDate, client);

        res.type('html').send(html);
    } catch (error) {
        console.log(error);
    } finally {
        await client.close();
    }
});

const server = app.listen(port, () => console.log(`Example app listening on port ${port}!`));

server.keepAliveTimeout = 120 * 1000;
server.headersTimeout = 120 * 1000;
