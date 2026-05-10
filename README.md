# ngw_geofencer

## Docker

```bash
cp .env.example .env
docker compose up --build
```

Demo with simulator:

```bash
python ./init_demo_env.py
docker compose --profile demo up --build
```

Open:

```text
http://localhost:5000
```

## Local

Install:

```bash
cp .env.example .env
python -m venv env
. env/bin/activate
. env/Scripts/activate # Windows
pip install -r requirements.txt
cd web
npm install
```

Flask app:

```bash
cd geofencer
python app.py
```

Geofencer:

```bash
cd geofencer
python ngw_geofencer.py
```

Simulator:

```bash
cd geofencer
python geometry_modifier.py
```

Frontend dev server:

```bash
cd web
npm run dev
```
