<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body>

  <h1>ETL Project: Multi-Currency Market Capitalization for Top Banks</h1>

  <p>This project is the capstone of my ETL course on Coursera, where I applied data engineering skills to extract, transform, and load data for practical use by a multi-national firm. Here, I gathered real-world data on the top 10 largest banks in the world by market capitalization, transformed the data into multiple currencies, and stored it for flexible querying.</p>

  <h2>Project Overview</h2>

  <h3>Scenario</h3>
  <p>As a data engineer for a global firm, I was tasked with providing a list of the top 10 largest banks worldwide, ranked by market capitalization in USD. The data was then converted to GBP, EUR, and INR for local managers’ ease of access and decision-making.</p>

  <h3>Objectives</h3>
  <ul>
    <li><strong>Extraction</strong>: Scrape the bank data from a Wikipedia page and format it as a structured DataFrame.</li>
    <li><strong>Transformation</strong>: Convert the market capitalization values from USD to GBP, EUR, and INR using exchange rates from a CSV file.</li>
    <li><strong>Loading</strong>:
      <ul>
        <li>Save the transformed data to a CSV file.</li>
        <li>Load the data into a SQLite database table.</li>
      </ul>
    </li>
    <li><strong>Querying</strong>: Run SQL queries to retrieve information for different international offices based on their respective currency.</li>
  </ul>

  <h2>Data Workflow</h2>
  <ol>
    <li><strong>Extract</strong>: Data scraping of the bank table from a Wikipedia archive URL using <code>requests</code> and <code>BeautifulSoup</code>.</li>
    <li><strong>Transform</strong>: 
      <ul>
        <li>Use a CSV file with exchange rates to convert the USD values to GBP, EUR, and INR.</li>
        <li>Add new columns for these converted values and round them to 2 decimal places.</li>
      </ul>
    </li>
    <li><strong>Load</strong>:
      <ul>
        <li>Save the transformed data to a CSV file.</li>
        <li>Store it as a table in an SQLite database for further querying.</li>
      </ul>
    </li>
  </ol>

  <h2>Project Files</h2>
  <ul>
    <li><strong><code>ETL_script.py</code></strong>: Contains all functions for the ETL process, including extraction, transformation, loading, and querying.</li>
    <li><strong><code>exchange_rate.csv</code></strong>: Provides the exchange rates for USD to GBP, EUR, and INR.</li>
    <li><strong><code>Largest_banks_data.csv</code></strong>: The final CSV output containing the banks’ names and market capitalization in multiple currencies.</li>
    <li><strong><code>Banks.db</code></strong>: SQLite database storing the transformed data for easy querying.</li>
    <li><strong><code>code_log.txt</code></strong>: Log file tracking the progress of each ETL step.</li>
  </ul>

  <h2>Code Highlights</h2>
  <ul>
    <li><strong>Data Extraction</strong>: The <code>extract()</code> function fetches and parses HTML to extract relevant table data.</li>
    <li><strong>Data Transformation</strong>: The <code>transform()</code> function adds new columns for market cap in GBP, EUR, and INR.</li>
    <li><strong>Data Loading</strong>:
      <ul>
        <li><code>load_to_csv()</code> saves the data as a CSV.</li>
        <li><code>load_to_db()</code> stores the data in an SQLite database.</li>
      </ul>
    </li>
    <li><strong>Query Execution</strong>: <code>run_query()</code> runs SQL queries to retrieve location-specific data for offices in London, Berlin, and New Delhi.</li>
    <li><strong>Progress Logging</strong>: <code>log_progress()</code> logs each stage of the ETL process.</li>
  </ul>

  <h2>How to Run</h2>
  <ol>
    <li>Clone the repository.</li>
    <li>Ensure you have the necessary libraries installed (<code>requests</code>, <code>BeautifulSoup</code>, <code>pandas</code>, <code>numpy</code>, <code>sqlite3</code>).</li>
    <li>Run the ETL script: <code>python ETL_script.py</code></li>
    <li>Check <code>Largest_banks_data.csv</code> and <code>Banks.db</code> for output data, and view <code>code_log.txt</code> for the process log.</li>
  </ol>

  <h2>Queries & Sample Results</h2>
  <ul>
    <li>Retrieve the list of banks with their market capitalization in GBP for the London office.</li>
    <li>Fetch the list of banks with their market cap in EUR for the Berlin office.</li>
    <li>Obtain the market cap in INR for the New Delhi office.</li>
  </ul>

  <h2>License</h2>
  <p>This project is licensed under the MIT License.</p>

</body>
</html>
