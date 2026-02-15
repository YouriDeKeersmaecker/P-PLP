Hello there

python -m venv .venv

.\.venv\Scripts\Activate

pip install -e .[dev]

pytest

create .env : DATABASE_URL=postgresql+psycopg2://postgres:yourpassword@localhost:5432/synthea10

psycopg2-binary is the driver for postresql to python

Synthea
- small dataset: ./run_synthea -s 42 -p 100 -r 20250101 --exporter.csv.export=true --exporter.baseDirectory="./output_small/" 
- medium dataset: ./run_synthea -s 43 -p 1000 -r 20250101 --exporter.csv.export=true --exporter.baseDirectory="./output_medium/" 
- large dataset: ./run_synthea -s 44 -p 10000 -r 20250101 --exporter.csv.export=true --exporter.baseDirectory="./output_large/"