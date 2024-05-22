-- Load the data
reservations = LOAD '../../data/kafka/reservas.txt' USING PigStorage('\n') AS (line:chararray);

-- Extract the month-year from the date
reservations_filtered = FILTER reservations BY line MATCHES '.*Fecha Llegada: .*';
reservations_parsed = FOREACH reservations_filtered GENERATE
    ToString(Substring(line, 15, 10), 'yyyy-MM-dd') AS fecha_llegada;

-- Group by month-year
reservations_grouped = GROUP reservations_parsed BY ToString(fecha_llegada, 'yyyy-MM');

-- Count the reservations per month-year
reservations_count = FOREACH reservations_grouped GENERATE
    group AS month_year, COUNT(reservations_parsed) AS count;

-- Store the result
STORE reservations_count INTO '/path/to/output' USING PigStorage(',');
