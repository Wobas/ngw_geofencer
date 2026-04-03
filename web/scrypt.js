const axios = require('axios');

async function sendDataToFlask() {
    try {
        const response = await axios.post('http://localhost:5000/api/data', {
            message: 'Hello from Node.js',
            timestamp: Date.now()
        });
        console.log(response.data);
    } catch (error) {
        console.error('Error:', error.message);
    }
}

async function getDataFromFlask() {
    try {
        const response = await axios.get('http://localhost:5000/api/predict');
        console.log(response.data);
    } catch (error) {
        console.error('Error:', error.message);
    }
}