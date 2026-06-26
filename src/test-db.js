require('dotenv').config();

const { testConnection, gracefulShutdown } = require('./db');

(async () => {
  try {
    await testConnection();
    console.log('Database connection successful');
    await gracefulShutdown('TEST');
    process.exit(0);
  } catch (err) {
    console.error(err.message);
    process.exit(1);
  }
})();