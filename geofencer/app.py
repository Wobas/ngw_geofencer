import os
from datetime import datetime
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
import data_broker

app = Flask(__name__)
CORS(app)

LOG_FILENAME = 'geofencing.log'

def run_flask():
    app.run(debug=False, host='0.0.0.0', port=5000, use_reloader=False)

@app.route('/')
def hello():
    return "Hello, World!"

@app.route('/logs_all', methods=['GET'])
def get_logs_all():
    """
    The endpoint for getting logs for the entire monitoring time.
    
    Request example:
    GET /logs_all
    """
    try:
        if not os.path.exists(data_broker.DB_PATH):
            return jsonify({
                'error': f'Data base not found: {data_broker.DB_PATH}',
                'current_directory': os.getcwd()
            }), 404
        rows = data_broker.read_all_events()
        return jsonify({
            'total_count': len(rows),
            'logs': [
                {
                    "timestamp": row[0],
                    "message": row[1],
                    "geom1": row[2],
                    "geom2": row[3]
                }
                for row in rows
            ]
        }), 200
    except Exception as e:
        app.logger.error(f"Error in logs_all: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500

@app.route('/logs_last', methods=['GET'])
def get_logs_last():
    """
    """
    try:
        if not os.path.exists(data_broker.DB_PATH):
            return jsonify({
                'error': f'Data base not found: {data_broker.DB_PATH}',
                'current_directory': os.getcwd()
            }), 404
        row = data_broker.read_last_event()
        return jsonify({
            'logs': [
                {
                    "timestamp": row[0],
                    "message": row[1],
                    "geom1": row[2],
                    "geom2": row[3]
                }
            ]
        }), 200
    except Exception as e:
        app.logger.error(f"Error in logs_all: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500

@app.route('/logs_range', methods=['GET'])
def get_logs_by_date_range_post():
    """
    The endpoint for getting logs for the period.
    
    Query parameters:
    - start: start date-time (format: YYYY-MM-DD HH:MM:SS)
    - end: end date-time (format: YYYY-MM-DD HH:MM:SS)
    
    Request example:
    GET /logs?start=2025-01-01 00:00:00&end=2025-12-31 23:59:59
    """
    start_str = request.args.get('start')
    end_str = request.args.get('end')
    
    if not start_str or not end_str:
        return jsonify({
            'error': 'The start and end parameters must be passed',
            'example': '/logs?start=2025-01-01 00:00:00&end=2025-12-31 23:59:59'
        }), 400
    
    try:
        start_date = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
        
        if start_date > end_date:
            return jsonify({
                'error': 'The start date cannot be later than the end date'
            }), 400
        
    except ValueError as e:
        return jsonify({
            'error': 'Incorrect date format. Use: YYYY-MM-DD HH:MM:SS',
            'detail': str(e)
        }), 400
    

    try:
        if not os.path.exists(data_broker.DB_PATH):
            return jsonify({
                'error': f'Data base not found: {data_broker.DB_PATH}',
                'current_directory': os.getcwd()
            }), 404
        row = data_broker.read_datetime_range_events(start_date.timestamp(), end_date.timestamp())
        return jsonify({
            'logs': [
                {
                    "timestamp": row[0],
                    "message": row[1],
                    "geom1": row[2],
                    "geom2": row[3]
                }
            ]
        }), 200
    except Exception as e:
        app.logger.error(f"Error in logs_all: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500


def logs_to_dataframe(log_file: str = LOG_FILENAME):
    """
    
    """
    logs = []
    
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                time_str = line[:23]
                timestamp = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S,%f')
                message = line[24:].strip() 
                logs.append({'timestamp': timestamp, 'message': message})
            except (ValueError, IndexError):
                continue
    
    df = pd.DataFrame(logs)
    return df


if __name__ == '__main__':
    run_flask()
