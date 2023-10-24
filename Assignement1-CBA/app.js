// Assignment-1: Cloud-Based-Applications
const { DateTime } = require("luxon");
const http = require('http');
var express = require('express');
var app = express();
const fs = require('fs');
const parse = require('csv-parser');

const hostname = '0.0.0.0';
const port = 8080;
const filePath = "./LU1_DF_B1100_1.0_C01.A.csv"


app.use(express.json());

app.get('/', async (req, res) => {
    const date = req.query.date;
    let dateAsInteger = parseInt(date, 10)

    if(dateAsInteger >= 1800 && dateAsInteger <= 2050){
        iterateData(dateAsInteger)
            .then((data) => {
                if (data.length !== 0){
                    res.status(201).send(data);
                } else {
                    res.status(404).send('No data found.')
                }
            }) 
            .catch(() => {
                res.status(500).send('Internal Server Error.')
            });
        
    } else {
        res.status(400).send('Invalid data.')
    }
});

app.listen(port, hostname, () => {
    console.log(`Server running at: http://${hostname}:${port}/`)
});

function iterateData(query) {
    return new Promise((resolve, reject) => { 
        const results = [];
        const beforeDate = { date: null, diff: Infinity };
        const afterDate = { date: null, diff: Infinity };
        
        fs.createReadStream(filePath)
            .pipe(parse({ 
                delimiter: ',',
                headers: ['', '', '', 'TIME_PERIOD', 'OBS_VALUE'],
                skipLines: 1
            }))
            .on('data', (row) => {
                const { TIME_PERIOD, OBS_VALUE } = row;
                const parsedDate = DateTime.fromFormat(TIME_PERIOD, 'yyyy-MM-dd');
                let year = parsedDate.year;

                if (year === query) {
                    results.push({ TIME_PERIOD: parsedDate.toISODate(), OBS_VALUE });
                } else if (year < query) {
                    if (!beforeDate.date || parsedDate > beforeDate.date) {
                        beforeDate.date = parsedDate;
                        beforeDate.OBS_VALUE = OBS_VALUE;
                    }
                } else if (year > query) {
                    if (!afterDate.date || parsedDate < afterDate.date) {
                        afterDate.date = parsedDate;
                        afterDate.OBS_VALUE = OBS_VALUE;
                    }
                }
            })
            .on('end', () => {
                if (results.length === 0) {
                    if (beforeDate.date) {
                        results.push({ TIME_PERIOD: beforeDate.date.toISODate(), OBS_VALUE: beforeDate.OBS_VALUE });
                    }
                    if (afterDate.date) {
                        results.push({ TIME_PERIOD: afterDate.date.toISODate(), OBS_VALUE: afterDate.OBS_VALUE });
                    }
                }
                resolve(results);
            })
            .on('error', (err) => {
                reject(err)
            });
    });
}

