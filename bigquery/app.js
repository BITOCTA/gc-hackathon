const express = require("express");
const cors = require("cors"); // Import the cors library

const app = express();

const port = process.env.PORT || 3000;

const {BigQuery} = require("@google-cloud/bigquery");
const bigquery = new BigQuery();

app.use(express.json());
app.use(cors());


async function performQuery(q){
    const options = {
    query: q,
    location: "europe-north1"
  };


  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);
  console.log(`Job ${job.id} started.`);

  // Wait for the query to finish
  const [rows] = await job.getQueryResults();



  return rows
}



app.get('/points', async(req, res) => {

   const latRound = req.query.latRound || 2;
  const longRound = req.query.longRound || 2;

  const query = `WITH GridPoints AS (
  SELECT 
    ROUND(latitude, ${latRound}) as grid_lat,
    ROUND(longitude, ${longRound}) as grid_long,
    *
  FROM \`cloud-summit-waw23-1483.telemetry_records.fetched_data\`
)

, AggregatedData AS (
  SELECT 
    grid_lat,
    grid_long,
    AVG(no2_ppb) as avg_no2_ppb,
    AVG(co_ppm) as avg_co_ppm,
    AVG(pm25_ugm3) as avg_pm25
  FROM 
    GridPoints
  GROUP BY 
    grid_lat, 
    grid_long
)

SELECT 
  grid_lat,
  grid_long,
  avg_no2_ppb,
  CASE 
    WHEN avg_no2_ppb IS NULL THEN 'Undefined'
    WHEN avg_no2_ppb <= 53 THEN 'Green'
    WHEN avg_no2_ppb > 53 AND avg_no2_ppb <= 100 THEN 'Yellow'
    ELSE 'Red'
  END AS no2_quality,
  avg_co_ppm,
  CASE 
    WHEN avg_co_ppm IS NULL THEN 'Undefined'
    WHEN avg_co_ppm <= 9 THEN 'Green'
    WHEN avg_co_ppm > 9 AND avg_co_ppm <= 15 THEN 'Yellow'
    ELSE 'Red'
  END AS co_quality,
  avg_pm25,
  CASE 
    WHEN avg_pm25 IS NULL THEN 'Undefined'
    WHEN avg_pm25 <= 12 THEN 'Green'
    WHEN avg_pm25 > 12 AND avg_pm25 <= 35.4 THEN 'Yellow'
    ELSE 'Red'
  END AS pm25_quality
FROM 
  AggregatedData
`;

  return res.send(await performQuery(query))
})

app.get('/points/time', async(req, res) => {

  const latRound = req.query.latRound || 2;
  const longRound = req.query.longRound || 2;

  const query = `WITH GridPoints AS (
  SELECT 
    ROUND(latitude, ${latRound}) as grid_lat,
    ROUND(longitude, ${longRound}) as grid_long,
    TIMESTAMP_TRUNC(timestamp, HOUR) as hour_group,
    *
  FROM \`cloud-summit-waw23-1483.telemetry_records.fetched_data\`
)

, AggregatedData AS (
  SELECT 
    grid_lat,
    grid_long,
    hour_group,
    AVG(no2_ppb) as avg_no2_ppb,
    AVG(co_ppm) as avg_co_ppm,
    AVG(pm25_ugm3) as avg_pm25
  FROM 
    GridPoints
  GROUP BY 
    grid_lat, 
    grid_long,
    hour_group
)

SELECT 
  grid_lat,
  grid_long,
  hour_group,
  avg_no2_ppb,
  CASE 
    WHEN avg_no2_ppb IS NULL THEN 'Undefined'
    WHEN avg_no2_ppb <= 53 THEN 'Green'
    WHEN avg_no2_ppb > 53 AND avg_no2_ppb <= 100 THEN 'Yellow'
    ELSE 'Red'
  END AS no2_quality,
  avg_co_ppm,
  CASE 
    WHEN avg_co_ppm IS NULL THEN 'Undefined'
    WHEN avg_co_ppm <= 9 THEN 'Green'
    WHEN avg_co_ppm > 9 AND avg_co_ppm <= 15 THEN 'Yellow'
    ELSE 'Red'
  END AS co_quality,
  avg_pm25,
  CASE 
    WHEN avg_pm25 IS NULL THEN 'Undefined'
    WHEN avg_pm25 <= 12 THEN 'Green'
    WHEN avg_pm25 > 12 AND avg_pm25 <= 35.4 THEN 'Yellow'
    ELSE 'Red'
  END AS pm25_quality
FROM 
  AggregatedData
`;

  return res.send(await performQuery(query))
})




app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});