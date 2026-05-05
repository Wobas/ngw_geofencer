# ngw_geofencer
NGW geofencer project

First, you need to initialize the file with the environment variables (.env).  
To do this, open the command prompt in the root of the project and run config.py:

```bash
python .\config.py
```

Now you are ready to launch the demo of the project.  
To do this, you have to switch current directory to the geofencer. So, use following command:

```bash
cd .\geofencer\
```

Now you should build and run docker image:

```bash
docker build -t ngw-geofencer .
docker run -p 5000:5000 ngw-geofencer
```

# View logs
To view logs open following link while docker is running: 
- [all_logs](http://localhost:5000/logs_all)
- [last_log](http://localhost:5000/logs_last)
