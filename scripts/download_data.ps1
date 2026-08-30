$url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
$output = "data\raw\yellow_tripdata_2024-01.parquet"
Invoke-WebRequest -Uri $url -OutFile $output
Write-Host "Download completed: $output"