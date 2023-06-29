const Sequelize = require('sequelize');

const sequelize = new Sequelize("test", process.argv[3], process.argv[4], {
    host: process.argv[2],
    port: 4000,
    dialect: 'mysql',
    dialectOptions: {
        ssl: {
            minVersion: 'TLSv1.2',
            rejectUnauthorized: true
        }
    }
});

(async () => {
    try {
        const [results, _] = await sequelize.query("SHOW DATABASES");
        console.log(results);
    } catch (err) {
        console.error("error executing query:", err);
    } finally {
        await sequelize.close();
    }
})();
