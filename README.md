# onderwijsinstellingen

#### setup:
docker-compose build && docker-compose up -d referentie_database && docker-compose up dataprocessor

#### run logic
docker exec -ti dataprocessor bash
python dataprocessing/dataprocessor.py
