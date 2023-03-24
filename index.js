// Import required modules
require('dotenv').config();
const express = require('express');
const csv = require('csv-parser');
const fs = require('fs');
const { Pool } = require('pg');


// Create an Express app
const app = express();

// Set the port number
const port = process.env.PORT || 3000;

const dbConfig = {
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_DATABASE,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
};
// Configure PostgreSQL database connection
const pool = new Pool(dbConfig);

// Define the API endpoint for CSV to JSON conversion
app.post('/convert', (req, res) => {
  const filePath = req.body.filePath; // The path of the CSV file to be converted
  
  // Read the CSV file and parse its data
  const results = [];
  fs.createReadStream(filePath)
    .pipe(csv())
    .on('data', (data) => {
      // Map mandatory properties to designated fields of the table
      const name = data['name.firstName'] + ' ' + data['name.lastName'];
      const age = parseInt(data['age']);
      const address = {
        line1: data['address.line1'],
        line2: data['address.line2'],
        city: data['address.city'],
        state: data['address.state'],
      };
      
      // Put remaining properties to additional_info field as a JSON object
      const additional_info = {};
      for (const key in data) {
        if (key !== 'name.firstName' && key !== 'name.lastName' && key !== 'age' && key !== 'address.line1' && key !== 'address.line2' && key !== 'address.city' && key !== 'address.state') {
          additional_info[key] = data[key];
        }
      }
      
      // Create a JSON object for the current CSV row
      const json = {
        name: name,
        age: age,
        address: address,
        additional_info: additional_info,
      };
      
      results.push(json);
    })
    .on('end', () => {
      // Save the JSON objects to the PostgreSQL database
      const query = 'INSERT INTO public.users(name, age, address, additional_info) VALUES($1, $2, $3, $4) RETURNING *';
      results.forEach((result) => {
        const values = [result.name, result.age, JSON.stringify(result.address), JSON.stringify(result.additional_info)];
        pool.query(query, values, (err, res) => {
          if (err) {
            console.error(err);
          } else {
            console.log(res.rows[0]);
          }
        });
      });
      res.send('CSV file converted to JSON and saved to database successfully!');
    });
});

// Start the server
app.listen(port, () => {
  console.log(`Server started on port ${port}`);
});
