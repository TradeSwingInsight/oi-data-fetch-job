const { format, parse } = require('date-fns');
const round = require('lodash.round');

const fetchDataNSE = async (indexName, client) => {
    try {
        let result = await fetch(`https://www.nseindia.com/api/option-chain-indices?symbol=${indexName}`,{
            headers: {
                "User-Agent": "PostmanRuntime/7.36.1",
                "Accept": "*/*",
                "Postman-Token": "701b9104-0731-46d4-8f11-924f5b0cf030",
                "Host": "www.nseindia.com",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            }
        })
        result = await result.json();
        
        const lastPrice = result.records.underlyingValue || 0;
        const timestamp = parse(result.records.timestamp, 'dd-MMM-yyyy HH:mm:ss', new Date());
        const syncDate = format(timestamp, 'dd-MM-yyyy');
        const syncTime = format(timestamp, 'p');
        let ocData = result.filtered.data;
        const expiryDate = ocData.length > 0 ? ocData[0].expiryDate : '';

        const oiDataDB = client.db("oiDataDB");
        const oiDataColl = oiDataDB.collection("oiData");

        const isExist = await oiDataColl.countDocuments({
            syncDate,
            syncTime,
            expiryDate,
            indexName
        });

        if (isExist !== 0) {
            return;
        }

        const closest = ocData.reduce((prev, curr) => {
            return Math.abs(curr.strikePrice - lastPrice) < Math.abs(prev - lastPrice) ? curr.strikePrice : prev;
        }, 0);

        const index = ocData.findIndex((item) => {
            return item.strikePrice === closest;
        });

        ocData = ocData.slice(index - 11, index + 12);

        let CEOI = 0;
        let PEOI = 0;

        let CETotalChangeInOI = 0;
        let PETotalChangeInOI = 0;

        let CETotalTradedVolume = 0;
        let PETotalTradedVolume = 0;

        const newData = ocData.map((item) => {
            const pcrChangeInOI = round(item.PE.changeinOpenInterest / item.CE.changeinOpenInterest, 2);
            const pcrOI = round(item.PE.openInterest / item.CE.openInterest, 2);
            
            CEOI += item.CE.openInterest;
            PEOI += item.PE.openInterest;

            CETotalChangeInOI += item.CE.changeinOpenInterest;
            PETotalChangeInOI += item.PE.changeinOpenInterest;

            CETotalTradedVolume += item.CE.totalTradedVolume;
            PETotalTradedVolume += item.PE.totalTradedVolume;
            
            return {
                pcrChangeInOI,
                pcrOI,
                strikePrice: item.strikePrice,
                PE: {
                    openInterest: item.PE.openInterest,
                    changeinOpenInterest: item.PE.changeinOpenInterest,
                    totalTradedVolume: item.PE.totalTradedVolume,
                    impliedVolatility: item.PE.impliedVolatility,
                    lastPrice: item.PE.lastPrice,
                    pchangeinOpenInterest: item.PE.pchangeinOpenInterest
                },
                CE: {
                    openInterest: item.CE.openInterest,
                    changeinOpenInterest: item.CE.changeinOpenInterest,
                    totalTradedVolume: item.CE.totalTradedVolume,
                    impliedVolatility: item.CE.impliedVolatility,
                    lastPrice: item.CE.lastPrice,
                    pchangeinOpenInterest: item.CE.pchangeinOpenInterest,
                }
            };
        });

        const insert = {
            syncDate,
            syncTime,
            lastPrice,
            closest,
            indexName,
            expiryDate,
            CEOI,
            PEOI,
            CETotalChangeInOI,
            PETotalChangeInOI,
            CETotalTradedVolume,
            PETotalTradedVolume,
            pcrChangeInOI: round(PETotalChangeInOI / CETotalChangeInOI, 2),
            pcrOI: round(PEOI / CEOI, 2),
            details: newData
        }

        const inserted = await oiDataColl.insertOne(insert);
        console.log("DONE");
        return inserted.insertedId;
    } catch (error) {
        console.log(error.message);
    }
};

const fetchDataBSE = async (indexName, expiryDate, client) => {
    try {
        expiryDate = parse(expiryDate, 'ddMMyyyy', new Date());
        expiryDate = format(expiryDate, 'dd+MMM+yyyy');

        const url = `https://api.bseindia.com/BseIndiaAPI/api/DerivOptionChain_IV/w?Expiry=${expiryDate}&scrip_cd=1&strprice=0`;

        let oiData = await fetch(url,{
            headers: {
                "accept": "application/json, text/plain, */*",
                "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
                "sec-ch-ua": "\"Not A(Brand\";v=\"99\", \"Google Chrome\";v=\"121\", \"Chromium\";v=\"121\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"macOS\"",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-site",
                "Referer": "https://api.bseindia.com/",
                "Referrer-Policy": "strict-origin-when-cross-origin",
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            }
        })
        oiData = await oiData.json();

        const lastPrice = oiData.Table?.[0]?.UlaValue || 0;
        const timestamp = parse(oiData.ASON.DT_TM, 'dd MMM yyyy | HH:mm', new Date());
        const syncDate = format(timestamp, 'dd-MM-yyyy');
        const syncTime = format(timestamp, 'p');
        
        const oiDataDB = client.db("oiDataDB");
        const oiDataColl = oiDataDB.collection("oiData");

        const isExist = await oiDataColl.countDocuments({
            syncDate,
            syncTime,
            expiryDate,
            indexName
        });

        if (isExist !== 0) {
            return;
        }

        const closest = oiData.Table.reduce((prev, curr) => {
            return Math.abs(parseFloat(curr.Strike_Price1) - parseFloat(curr.UlaValue)) < Math.abs(parseFloat(prev) - parseFloat(curr.UlaValue)) ? parseFloat(curr.Strike_Price1) : prev;
        }, 0);

        const index = oiData.Table.findIndex((item) => {
            return parseFloat(item.Strike_Price1) === closest;
        });


        const actualData = oiData.Table.slice(index - 11, index + 12);

        let CETotalChangeInOI = 0;
        let PETotalChangeInOI = 0;

        let CETotalTradedVolume = 0;
        let PETotalTradedVolume = 0;

        let CEOI = 0;
        let PEOI = 0;

        const newData = actualData.map((item) => {
            const changeinOpenInterestPE = parseFloat(item.Absolute_Change_OI);
            const changeinOpenInterestCE = parseFloat(item.C_Absolute_Change_OI);

            const totalTradedVolumePE = parseInt(item.Vol_Traded);
            const totalTradedVolumeCE = parseInt(item.C_Vol_Traded);

            const openInterestPE = parseInt(item.Open_Interest);
            const openInterestCE = parseInt(item.C_Open_Interest);

            const pcrChangeInOI = round(changeinOpenInterestPE / changeinOpenInterestCE, 2) || 0;
            const pcrOI = round(openInterestPE / openInterestCE, 2);
            
            CETotalChangeInOI += changeinOpenInterestCE;
            PETotalChangeInOI += changeinOpenInterestPE;

            CETotalTradedVolume += totalTradedVolumeCE;
            PETotalTradedVolume += totalTradedVolumePE;

            CEOI += openInterestCE;
            PEOI += openInterestPE;

            return {
                pcrChangeInOI,
                pcrOI,
                strikePrice: parseFloat(item.Strike_Price1),
                PE: {
                    openInterest: openInterestPE,
                    changeinOpenInterest: changeinOpenInterestPE,
                    totalTradedVolume: totalTradedVolumePE,
                    impliedVolatility: round(parseFloat(item.IV), 2),
                    lastPrice: parseFloat(item.Last_Trd_Price),
                    pchangeinOpenInterest: '-'
                },
                CE: {
                    openInterest: openInterestCE,
                    changeinOpenInterest: changeinOpenInterestCE,
                    totalTradedVolume: totalTradedVolumeCE,
                    impliedVolatility: round(parseFloat(item.C_IV), 2),
                    lastPrice: parseFloat(item.C_Last_Trd_Price),
                    pchangeinOpenInterest: '-',
                }
            };
        });

        const insert = {
            syncDate,
            syncTime,
            lastPrice,
            closest,
            indexName,
            expiryDate,
            CEOI,
            PEOI,
            CETotalChangeInOI,
            PETotalChangeInOI,
            CETotalTradedVolume,
            PETotalTradedVolume,
            pcrChangeInOI: round(PETotalChangeInOI / CETotalChangeInOI, 2),
            pcrOI: round(PEOI / CEOI, 2),
            details: newData
        };

        const inserted = await oiDataColl.insertOne(insert);
        console.log("DONE");
        return inserted.insertedId;
        
    } catch (error) {
        console.log(error);
    }
};

module.exports = {
    fetchDataNSE,
    fetchDataBSE
};