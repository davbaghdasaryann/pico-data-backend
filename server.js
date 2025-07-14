const express = require('express');
const { MongoClient } = require('mongodb');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3001;

// MongoDB connection string
const MONGODB_URI = 'mongodb+srv://admin:JEFVJr27n25cdlxI0RQ1@mongo.iotlb.online/admin?authSource=admin&replicaSet=rs0&tls=false';

// Middleware
app.use(cors());
app.use(express.json());

// MongoDB connection
let db;
MongoClient.connect(MONGODB_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(client => {
  console.log('Connected to MongoDB');
  db = client.db('mqtt_data');
})
.catch(error => {
  console.error('Failed to connect to MongoDB:', error);
  process.exit(1);
});

// Helper function to handle database queries
const handleDatabaseQuery = async (res, queryFunction) => {
  try {
    const result = await queryFunction();
    res.json(result);
  } catch (error) {
    console.error('Database query error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

// === AIR POLLUTION ENDPOINTS ===

// Get all air pollution data
app.get('/api/air-pollution', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    const { limit = 100, skip = 0, sort = 'created_at' } = req.query;
    
    const data = await db.collection('air_polution')
      .find({})
      .sort({ [sort]: -1 })
      .skip(parseInt(skip))
      .limit(parseInt(limit))
      .toArray();
    
    return {
      data,
      count: data.length,
      total: await db.collection('air_polution').countDocuments()
    };
  });
});

// Get latest air pollution reading
app.get('/api/air-pollution/latest', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    return await db.collection('air_polution')
      .findOne({}, { sort: { created_at: -1 } });
  });
});

// Get air pollution data by topic
app.get('/api/air-pollution/topic/:topic', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    const { topic } = req.params;
    const { limit = 100, skip = 0 } = req.query;
    
    const data = await db.collection('air_polution')
      .find({ topic })
      .sort({ created_at: -1 })
      .skip(parseInt(skip))
      .limit(parseInt(limit))
      .toArray();
    
    return {
      data,
      count: data.length,
      topic
    };
  });
});

// Get air pollution data by air quality level
app.get('/api/air-pollution/quality/:level', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    const { level } = req.params;
    const { limit = 100, skip = 0 } = req.query;
    
    const data = await db.collection('air_polution')
      .find({ 'air_quality.level': parseInt(level) })
      .sort({ created_at: -1 })
      .skip(parseInt(skip))
      .limit(parseInt(limit))
      .toArray();
    
    return {
      data,
      count: data.length,
      quality_level: parseInt(level)
    };
  });
});

// Get air pollution data within date range
app.get('/api/air-pollution/range', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    const { start_date, end_date, limit = 100, skip = 0 } = req.query;
    
    let filter = {};
    if (start_date || end_date) {
      filter.created_at = {};
      if (start_date) filter.created_at.$gte = new Date(start_date);
      if (end_date) filter.created_at.$lte = new Date(end_date);
    }
    
    const data = await db.collection('air_polution')
      .find(filter)
      .sort({ created_at: -1 })
      .skip(parseInt(skip))
      .limit(parseInt(limit))
      .toArray();
    
    return {
      data,
      count: data.length,
      filter
    };
  });
});

// === SOIL MOISTURE ENDPOINTS ===

// Get all soil moisture data
app.get('/api/soil-moisture', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    const { limit = 100, skip = 0, sort = 'created_at' } = req.query;
    
    const data = await db.collection('soil_moisture')
      .find({})
      .sort({ [sort]: -1 })
      .skip(parseInt(skip))
      .limit(parseInt(limit))
      .toArray();
    
    return {
      data,
      count: data.length,
      total: await db.collection('soil_moisture').countDocuments()
    };
  });
});

// Get latest soil moisture reading
app.get('/api/soil-moisture/latest', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    return await db.collection('soil_moisture')
      .findOne({}, { sort: { created_at: -1 } });
  });
});

// Get soil moisture data by topic
app.get('/api/soil-moisture/topic/:topic', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    const { topic } = req.params;
    const { limit = 100, skip = 0 } = req.query;
    
    const data = await db.collection('soil_moisture')
      .find({ topic })
      .sort({ created_at: -1 })
      .skip(parseInt(skip))
      .limit(parseInt(limit))
      .toArray();
    
    return {
      data,
      count: data.length,
      topic
    };
  });
});

// Get soil moisture data by pin
app.get('/api/soil-moisture/pin/:pin', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    const { pin } = req.params;
    const { limit = 100, skip = 0 } = req.query;
    
    const data = await db.collection('soil_moisture')
      .find({ pin })
      .sort({ created_at: -1 })
      .skip(parseInt(skip))
      .limit(parseInt(limit))
      .toArray();
    
    return {
      data,
      count: data.length,
      pin
    };
  });
});

// Get soil moisture data by soil status level
app.get('/api/soil-moisture/status/:level', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    const { level } = req.params;
    const { limit = 100, skip = 0 } = req.query;
    
    const data = await db.collection('soil_moisture')
      .find({ 'soil_status.level': parseInt(level) })
      .sort({ created_at: -1 })
      .skip(parseInt(skip))
      .limit(parseInt(limit))
      .toArray();
    
    return {
      data,
      count: data.length,
      status_level: parseInt(level)
    };
  });
});

// Get soil moisture data within date range
app.get('/api/soil-moisture/range', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    const { start_date, end_date, limit = 100, skip = 0 } = req.query;
    
    let filter = {};
    if (start_date || end_date) {
      filter.created_at = {};
      if (start_date) filter.created_at.$gte = new Date(start_date);
      if (end_date) filter.created_at.$lte = new Date(end_date);
    }
    
    const data = await db.collection('soil_moisture')
      .find(filter)
      .sort({ created_at: -1 })
      .skip(parseInt(skip))
      .limit(parseInt(limit))
      .toArray();
    
    return {
      data,
      count: data.length,
      filter
    };
  });
});

// === ANALYTICS ENDPOINTS ===

// Get air pollution statistics
app.get('/api/air-pollution/stats', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    const stats = await db.collection('air_polution').aggregate([
      {
        $group: {
          _id: null,
          avgTemperature: { $avg: '$temperature' },
          avgHumidity: { $avg: '$humidity' },
          avgPM10: { $avg: '$pm10' },
          avgPM25: { $avg: '$pm25' },
          maxTemperature: { $max: '$temperature' },
          minTemperature: { $min: '$temperature' },
          maxHumidity: { $max: '$humidity' },
          minHumidity: { $min: '$humidity' },
          totalReadings: { $sum: 1 }
        }
      }
    ]).toArray();
    
    return stats[0] || {};
  });
});

// Get soil moisture statistics
app.get('/api/soil-moisture/stats', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    const stats = await db.collection('soil_moisture').aggregate([
      {
        $group: {
          _id: null,
          avgMoisture: { $avg: '$soil_moisture_percent' },
          avgRaw: { $avg: '$soil_moisture_raw' },
          maxMoisture: { $max: '$soil_moisture_percent' },
          minMoisture: { $min: '$soil_moisture_percent' },
          totalReadings: { $sum: 1 }
        }
      }
    ]).toArray();
    
    return stats[0] || {};
  });
});

// Get air quality distribution
app.get('/api/air-pollution/quality-distribution', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    return await db.collection('air_polution').aggregate([
      {
        $group: {
          _id: '$air_quality.status',
          count: { $sum: 1 },
          level: { $first: '$air_quality.level' }
        }
      },
      { $sort: { level: 1 } }
    ]).toArray();
  });
});

// Get soil status distribution
app.get('/api/soil-moisture/status-distribution', async (req, res) => {
  await handleDatabaseQuery(res, async () => {
    return await db.collection('soil_moisture').aggregate([
      {
        $group: {
          _id: '$soil_status.status',
          count: { $sum: 1 },
          level: { $first: '$soil_status.level' }
        }
      },
      { $sort: { level: 1 } }
    ]).toArray();
  });
});

// === HEALTH CHECK ===
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    database: db ? 'Connected' : 'Disconnected'
  });
});

// === API DOCUMENTATION ===
app.get('/api', (req, res) => {
  res.json({
    name: 'IoT Sensor Data API',
    version: '1.0.0',
    endpoints: {
      air_pollution: {
        'GET /api/air-pollution': 'Get all air pollution data with pagination',
        'GET /api/air-pollution/latest': 'Get latest air pollution reading',
        'GET /api/air-pollution/topic/:topic': 'Get data by topic',
        'GET /api/air-pollution/quality/:level': 'Get data by air quality level',
        'GET /api/air-pollution/range': 'Get data within date range',
        'GET /api/air-pollution/stats': 'Get air pollution statistics',
        'GET /api/air-pollution/quality-distribution': 'Get air quality distribution'
      },
      soil_moisture: {
        'GET /api/soil-moisture': 'Get all soil moisture data with pagination',
        'GET /api/soil-moisture/latest': 'Get latest soil moisture reading',
        'GET /api/soil-moisture/topic/:topic': 'Get data by topic',
        'GET /api/soil-moisture/pin/:pin': 'Get data by pin',
        'GET /api/soil-moisture/status/:level': 'Get data by soil status level',
        'GET /api/soil-moisture/range': 'Get data within date range',
        'GET /api/soil-moisture/stats': 'Get soil moisture statistics',
        'GET /api/soil-moisture/status-distribution': 'Get soil status distribution'
      },
      general: {
        'GET /api/health': 'API health check',
        'GET /api': 'API documentation'
      }
    },
    query_parameters: {
      limit: 'Number of records to return (default: 100)',
      skip: 'Number of records to skip (default: 0)',
      sort: 'Sort field (default: created_at)',
      start_date: 'Start date for range queries (ISO format)',
      end_date: 'End date for range queries (ISO format)'
    }
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`IoT Sensor Data API running on port ${PORT}`);
  console.log(`API documentation available at: http://localhost:${PORT}/api`);
  console.log(`Health check available at: http://localhost:${PORT}/api/health`);
});

module.exports = app;
